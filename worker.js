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
    Seq = require("sequelize"),
    sleep = require("sleep-promise");

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    // size of connection pool
    MAXCONNS = parseInt(process.env.MAXCONNS) || 3,
    // number of participants to calculate at once
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 1000,
    LOAD_TIMEOUT = parseInt(process.env.LOAD_TIMEOUT) || 5;  // s

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
        tags: ["backend", "cruncher"],
        json: true
    });


(async () => {
    let seq, rabbit, ch;

    // connect to rabbit & db
    while (true) {
        try {
            seq = new Seq(DATABASE_URI, {
                logging: false,
                max: MAXCONNS
            }),
            rabbit = await amqp.connect(RABBITMQ_URI, { heartbeat: 320 }),
            ch = await rabbit.createChannel();
            await ch.assertQueue("crunch", {durable: true});
            break;
        } catch (err) {
            logger.error("Error connecting", err);
            await sleep(5000);
        }
    }

    // load update SQL scripts; scripts use sequelize replacements
    // for the `participant_api_id` array
    const player_script = fs.readFileSync("crunch_player.sql", "utf8"),
        global_script = fs.readFileSync("crunch_global.sql", "utf8");

    // fill a buffer and execute an SQL on a bigger (> 1o) batch
    const participants_player = new Set(),
        participants = new Set(),
        // store the msgs that should be ACKed
        buffer = new Set();
    let timeout = undefined;

    // set maximum allowed number of unacked msgs
    await ch.prefetch(BATCHSIZE);
    ch.consume("crunch", (msg) => {
        const api_id = msg.content.toString();
        if (msg.properties.type == "global")
            participants.add(api_id);
        // else exclusively add data to player, used for player refresh
        participants_player.add(api_id);
        buffer.add(msg);
        if (timeout == undefined) timeout = setTimeout(crunch, LOAD_TIMEOUT*1000);
        if (buffer.size >= BATCHSIZE) crunch();
    }, { noAck: false });

    // execute the scripts
    async function crunch() {
        const profiler = logger.startTimer();
        logger.info("crunching");

        // prevent async issues
        const api_ids_player = [...participants_player],
            api_ids = [...participants],
            msgs = new Set(buffer);
        participants.clear();
        participants_player.clear();
        buffer.clear();
        clearTimeout(timeout);
        timeout = undefined;

        if (api_ids.length > 0)
            await seq.query(global_script, {
                replacements: { participant_api_ids: api_ids },
                type: seq.QueryTypes.UPSERT
            });
        if (api_ids_player.length > 0)
            await seq.query(player_script, {
                replacements: { participant_api_ids: api_ids_player },
                type: seq.QueryTypes.UPSERT
            });

        // ack
        await Promise.map(msgs, async (m) => await ch.ack(m));
        // notify web
        // TODO notify for player too
        await ch.publish("amq.topic", "global", new Buffer("points_update"));

        profiler.done("crunched");
    }
})();

process.on("unhandledRejection", function(reason, promise) {
    logger.error(reason);
});

