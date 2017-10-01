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

    // array of all consumable ids
    const active_items = (await model.Item.findAll({
        where: { is_activable: true }
    })).map((i) => i.id);
    // returns an SQL snippet like this:
    /*      sum(column_get(p_i.item_grants, '14')) as item_014_use
     * or
     *      item_014_use = item_014_use + values(item_014_use)
     */
    const dynamic_sql = (doCreate, tableName) => {
        const pad = (i) => i.toString().padStart(3, "0");
        if (doCreate) {  // insert
            return active_items.map((i) =>
                `sum(column_get(${tableName}.item_uses, '${i}' as int)) ` +
                    `as item_${pad(i)}_use`
            ).join(",\n");
        } else {
            return active_items.map((i) =>
                `item_${pad(i)}_use = item_${pad(i)}_use + values(item_${pad(i)}_use)`
            ).join(",\n");
        }
    };
    // generate
    const
        p_i_item_uses_insert = dynamic_sql(true, 'p_i'),
        p_i_item_uses_update = dynamic_sql(false, 'p_i'),
        ph_item_uses_insert = dynamic_sql(true, 'ph'),
        ph_item_uses_update = dynamic_sql(false, 'ph')
    ;

    // replace stubs
    Object.entries({
        p_i_item_uses_insert,
        p_i_item_uses_update,
        ph_item_uses_insert,
        ph_item_uses_update
    }).forEach(([key, value]) => script = script.replace('_' + key, value));

    // fill a buffer and execute an SQL on a bigger (> 1o) batch
    const participants = new Set(),
        // store the msgs that should be ACKed
        buffer = new Set();
    let timeout = undefined;

    // set maximum allowed number of unacked msgs
    await ch.prefetch(BATCHSIZE);
    ch.consume(QUEUE, async (msg) => {
        if (msg.content.length > 1024) {
            // thx NodeJS for implementing `new Buffer(size)`
            // Sometimes I fuck up during message sending
            // and forget a `id.toString()`, creating huge blobs of zeros,
            // which makes Sequelize panic because the packets are 2MB.
            // May god forgive me for deploying debug code…
            await ch.nack(msg, false, false);
            return;
        }

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
