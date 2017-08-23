#!/usr/bin/node
/* jshint esnext:true */
"use strict";

/*
 * cruncher calculates global_point and player_point for stats.
 * It listens to the queue `crunch` and expects a JSON with
 * a string that is a `participant.api_id`.
 * cruncher will update the sums of points
 * and send a notification to web.
 */

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    fs = Promise.promisifyAll(require("fs")),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    sleep = require("sleep-promise"),
    Seq = require("sequelize");

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    QUEUE = process.env.QUEUE || "crunch",
    SCRIPT = process.env.SCRIPT || "crunch_global.sql",
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    // size of connection pool
    MAXCONNS = parseInt(process.env.MAXCONNS) || 3,
    // number of participants to calculate at once
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 1000,
    LOAD_TIMEOUT = parseInt(process.env.LOAD_TIMEOUT) || 5,  // s
    // wait time before next batch
    SLOWMODE = parseFloat(process.env.SLOWMODE) || 0;  // s

const logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({
                timestamp: true,
                colorize: true
            })
        ]
    });

// loggly integration
if (LOGGLY_TOKEN)
    logger.add(winston.transports.Loggly, {
        inputToken: LOGGLY_TOKEN,
        subdomain: "kvahuja",
        tags: ["backend", "cruncher", QUEUE],
        json: true
    });


amqp.connect(RABBITMQ_URI).then(async (rabbit) => {
    // connect to rabbit & db
    const seq = new Seq(DATABASE_URI, {
        logging: false,
        max: MAXCONNS
    });

    process.once("SIGINT", rabbit.close.bind(rabbit));

    const ch = await rabbit.createChannel();
    await ch.assertQueue(QUEUE, { durable: true });
    await ch.assertQueue(QUEUE + "_failed", { durable: true });

    const model = require("../orm/model")(seq, Seq);

    logger.info("configuration", {
        SCRIPT, QUEUE, BATCHSIZE, MAXCONNS, LOAD_TIMEOUT
    });

    // load update SQL scripts; scripts use sequelize replacements
    // for the `participant_api_id` array
    let script = fs.readFileSync(SCRIPT, "utf8");

    // array of all item ids
    const items = (await model.Item.findAll()).map((i) => i.id);
    // returns an SQL snippet like this:
    /* column_create(
     *      '1', column_get(p_i.item_grants, '1'),
     *      …
     *  )
     */
    const dynamic_sql = (doCreate, tableName, columnName) => {
        if (doCreate) {  // insert
            return `
column_create(` +
        (items.map((i) =>
            // create an array of `columnName, col_get($col, columnName)`
`   '${i}',
    coalesce(column_get(${tableName}.${columnName}, '${i}' as int), 0)`
        ).join(",\n")) + `
) as ${columnName}`;
        } else {
            return `
${columnName} = column_create(` +
        (items.map((i) =>
    // create an array of
    // `columnName, col_get($col, columnName) + col_get(values($col, columnName))`
`   '${i}',
    coalesce(column_get(${columnName}, '${i}' as int), 0)
    +
    coalesce(column_get(values(${columnName}), '${i}' as int), 0)`
        ).join(",\n")) + `
)`;
        }
    };
    // generate
    const
        p_i_items_insert = dynamic_sql(true, 'p_i', 'items'),
        p_i_item_grants_insert = dynamic_sql(true, 'p_i', 'item_grants'),
        p_i_item_uses_insert = dynamic_sql(true, 'p_i', 'item_uses'),
        p_i_item_sells_insert = dynamic_sql(true, 'p_i', 'item_sells'),
        p_i_items_update = dynamic_sql(false, 'p_i', 'items'),
        p_i_item_grants_update = dynamic_sql(false, 'p_i', 'item_grants'),
        p_i_item_uses_update = dynamic_sql(false, 'p_i', 'item_uses'),
        p_i_item_sells_update = dynamic_sql(false, 'p_i', 'item_sells'),
        ph_items_insert = dynamic_sql(true, 'ph', 'items'),
        ph_item_grants_insert = dynamic_sql(true, 'ph', 'item_grants'),
        ph_item_uses_insert = dynamic_sql(true, 'ph', 'item_uses'),
        ph_item_sells_insert = dynamic_sql(true, 'ph', 'item_sells'),
        ph_items_update = dynamic_sql(false, 'ph', 'items'),
        ph_item_grants_update = dynamic_sql(false, 'ph', 'item_grants'),
        ph_item_uses_update = dynamic_sql(false, 'ph', 'item_uses'),
        ph_item_sells_update = dynamic_sql(false, 'ph', 'item_sells')
    ;

    // replace stubs
    Object.entries({
        p_i_items_insert,
        p_i_item_grants_insert,
        p_i_item_uses_insert,
        p_i_item_sells_insert,
        p_i_items_update,
        p_i_item_grants_update,
        p_i_item_uses_update,
        p_i_item_sells_update,
        ph_items_insert,
        ph_item_grants_insert,
        ph_item_uses_insert,
        ph_item_sells_insert,
        ph_items_update,
        ph_item_grants_update,
        ph_item_uses_update,
        ph_item_sells_update,
    }).forEach(([key, value]) => script = script.replace('_' + key, value));

    // fill a buffer and execute an SQL on a bigger (> 1o) batch
    const participants = new Set(),
        // store the msgs that should be ACKed
        buffer = new Set();
    let timeout = undefined;

    // set maximum allowed number of unacked msgs
    await ch.prefetch(BATCHSIZE);
    ch.consume(QUEUE, async (msg) => {
        participants.add(msg.content.toString());
        buffer.add(msg);
        if (timeout == undefined) timeout = setTimeout(tryCrunch, LOAD_TIMEOUT*1000);
        if (buffer.size >= BATCHSIZE) await tryCrunch();
    }, { noAck: false });

    // wrap crunch() in message handler
    async function tryCrunch() {
        const msgs = new Set(buffer),
            api_ids = [...participants];

        buffer.clear();
        clearTimeout(timeout);
        timeout = undefined;

        participants.clear();

        if (SLOWMODE > 0) {
            logger.info("slowmode active, sleeping…", { wait: SLOWMODE });
            await sleep(SLOWMODE * 1000);
        }

        try {
            await crunch(api_ids);
        } catch (err) {
            // log, move to failed queue, NACK
            logger.error(err);
            await Promise.map(msgs, async (m) => {
                await ch.sendToQueue(QUEUE + "_failed", m.content, {
                    persistent: true,
                    headers: m.properties.headers
                });
                await ch.nack(m, false, false);
            });
            return;
        }

        await Promise.map(msgs, async (m) => await ch.ack(m));
        // notify web
        await Promise.map(msgs, async (m) => {
            if (m.properties.headers.notify)
                await ch.publish("amq.topic",
                    m.properties.headers.notify,
                    new Buffer("crunch_update")
                );
        });
    }

    // execute the script
    async function crunch(api_ids) {
        const profiler = logger.startTimer();
        logger.info("crunching", { size: api_ids.length });

        await seq.query(script, {
            replacements: {
                build_regex_start: '^([[:digit:]]+;[[:digit:]]+,)*(',
                build_regex_end: ')+(,[[:digit:]]+;[[:digit:]]+)*$',
                participant_api_ids: api_ids
            },
            type: seq.QueryTypes.UPSERT
        });

        profiler.done("crunched", { size: api_ids.length });
    }
});

process.on("unhandledRejection", (err) => {
    logger.error(err);
    process.exit(1);  // fail hard and die
});
