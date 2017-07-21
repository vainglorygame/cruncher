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
    CRUNCH_TABLE = process.env.CRUNCH_TABLE || "global_point",
    QUEUE = process.env.QUEUE || "crunch",
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
    const player_script = fs.readFileSync("crunch_player.sql", "utf8"),
        team_script = fs.readFileSync("crunch_team.sql", "utf8"),
        global_script = fs.readFileSync("crunch_global.sql", "utf8")
                          .replace("`global_point`", CRUNCH_TABLE);

    // fill a buffer and execute an SQL on a bigger (> 1o) batch
    const participants_player = new Set(),
        teams = new Set(),
        participants_global = new Set(),
        // store the msgs that should be ACKed
        buffer = new Set();
    let timeout = undefined;

    // set maximum allowed number of unacked msgs
    // TODO maybe split queues by type
    await ch.prefetch(BATCHSIZE);
    ch.consume(QUEUE, async (msg) => {
        const api_id = msg.content.toString();
        if (msg.properties.type == "global")
            participants_global.add(api_id);
        if (msg.properties.type == "player")
            participants_player.add(api_id);
        if (msg.properties.type == "team")
            teams.add(api_id);
        buffer.add(msg);
        if (timeout == undefined) timeout = setTimeout(tryCrunch, LOAD_TIMEOUT*1000);
        if (buffer.size >= BATCHSIZE) await tryCrunch();
    }, { noAck: false });

    // wrap crunch() in message handler
    async function tryCrunch() {
        const msgs = new Set(buffer),
            api_ids_global = [...participants_global],
            api_ids_player = [...participants_player],
            team_ids = [...teams];

        buffer.clear();
        clearTimeout(timeout);
        timeout = undefined;

        participants_global.clear();
        participants_player.clear();
        teams.clear();

        try {
            await crunch(api_ids_global, api_ids_player, team_ids);
        } catch (err) {
            // log, move to failed queue, NACK
            logger.error(err);
            await Promise.map(msgs, async (m) => {
                await ch.sendToQueue(QUEUE + "_failed", msg.content, {
                    persistent: true,
                    headers: msg.properties.headers
                });
                await msg.nack(false, false);
            });
            return;
        }

        await Promise.map(msgs, async (m) => await ch.ack(m));
        // notify web
        // TODO notify for player too
        await ch.publish("amq.topic", "global", new Buffer("points_update"));

        if (SLOWMODE > 0) {
            logger.info("slowmode active, sleeping…", { wait: SLOWMODE });
            await sleep(SLOWMODE * 1000);
        }
    }

    // execute the scripts
    async function crunch(api_ids_global, api_ids_player, team_ids) {
        const profiler = logger.startTimer();
        logger.info("crunching", {
            globals: api_ids_global.length,
            players: api_ids_player.length,
            teams: team_ids.length
        });

        if (api_ids_global.length > 0)
            await seq.query(global_script, {
                replacements: {
                    build_regex_start: '^([[:digit:]]+;[[:digit:]]+,)*(',
                    build_regex_end: ')+(,[[:digit:]]+;[[:digit:]]+)*$',
                    participant_api_ids: api_ids_global
                },
                type: seq.QueryTypes.UPSERT
            });
        if (team_ids.length > 0)
            await Promise.each(team_ids, async (tid) =>
                await seq.query(team_script, {
                    replacements: { team_id: tid },
                    type: seq.QueryTypes.UPDATE
                })
            );
        if (api_ids_player.length > 0)
            await seq.query(player_script, {
                replacements: { participant_api_ids: api_ids_player },
                type: seq.QueryTypes.UPSERT
            });

        profiler.done("crunched", {
            size: api_ids_global.length + api_ids_player.length + team_ids.length
        });
    }
});

process.on("unhandledRejection", (err) => {
    logger.error(err);
    process.exit(1);  // fail hard and die
});
