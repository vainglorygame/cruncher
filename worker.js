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
    SLOWMODE = parseInt(process.env.SLOWMODE) || 0;  // s

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

    // load update SQL scripts; scripts use sequelize replacements
    // for the `participant_api_id` array
    const script = fs.readFileSync(SCRIPT, "utf8");

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
        // TODO notify for player too
        // TODO fix these
        await ch.publish("amq.topic", "global", new Buffer("points_update"));

        if (SLOWMODE > 0) {
            logger.info("slowmode active, sleeping…", { wait: SLOWMODE });
            await sleep(SLOWMODE * 1000);
        }
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
