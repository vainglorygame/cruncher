#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    loggly = require("winston-loggly-bulk"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise"),
    hash = require("object-hash");

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    LOGGLY_TOKEN = process.env.LOGGLY_TOKEN,
    // number of inserts in one statement
    CHUNKSIZE = parseInt(process.env.CHUNKSIZE) || 300,
    MAXCONNS = parseInt(process.env.MAXCONNS) || 10,  // how many concurrent actions
    CRUNCHERS = parseInt(process.env.CRUNCHERS) || 4;  // how many players to crunch concurrently

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

// helpers
// split an array into arrays of max chunksize
function* chunks(arr) {
    for (let c=0, len=arr.length; c<len; c+=CHUNKSIZE)
        yield arr.slice(c, c+CHUNKSIZE);
}

(async () => {
    let seq, model, rabbit, ch;

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
    model = require("../orm/model")(seq, Seq);

    function cartesian(arr) {
        return Array.prototype.reduce.call(arr, function(a, b) {
            let ret = [];
            a.forEach(function(a) {
                b.forEach(function(b) {
                    ret.push(a.concat([b]));
                });
            });
            return ret;
        }, [[]]);
    }

    // create a 3D array
    // [[ ["hero", "Vox"], ["hero", "Taka"], …], [ ["game_mode", "ranked"], … ], …]
    // (will not use "Vox" but the index instead)
    async function dimensions_for(dimensions, on) {
        let cache = [];
        await Promise.map(dimensions, async (d, idx) =>
            cache[idx] = (await d.findAll()).map(
                (o) => [d, o]).filter((t) =>
                    t[1].get("dimension_on") == null
                        || t[1].get("dimension_on") == on)
        );
        return cache;
    }
    const player_dimensions = await dimensions_for(
        [model.Series, model.Filter, model.Hero, model.Role,
            model.GameMode], "player"),
        global_dimensions = await dimensions_for(
        [model.Series, model.Filter, model.Hero, model.Role,
            model.GameMode, model.Skilltier, model.Build,
            model.Region], "global");

    // return every possible [query, insert] combination
    function calculate_point(points) {
        return points.map((point) => {
            // Series and Filter are special
            let where_aggr = {},
                where_links = {};
            // create skeleton: where hero_id=$hero
            // for aggregation, use series as range
            // for links, use the id
            point.forEach((tuple) => {
                // [table name, table element]
                if (tuple[1].get("name") != "all") {
                    // exclude series & filter, added below
                    if (tuple[0].tableName == "filter") {
                        // merge custom filters
                        Object.assign(where_aggr,
                            tuple[1].get("filter"));
                    // series and skill_tier are ranged filters
                    } else if (tuple[0].tableName == "series") {
                        // use start < date < end comparison
                        where_aggr.created_at = { $between: [
                            tuple[1].get("start"),
                            tuple[1].get("end")
                        ] }
                    } else if (tuple[0].tableName == "skill_tier") {
                        where_aggr["$participant.skill_tier$"] = { $between: [
                            tuple[1].get("start"),
                            tuple[1].get("end")
                        ] }
                    // build is a special ranged filter
                    } else if (tuple[0].tableName == "build") {
                        // TODO!
                    } else if (tuple[0].tableName == "region") {
                        // not joined via id, joined via name
                        where_aggr["$participant.shard_id$"] = tuple[1].get("name");
                    // most filters are directly as $filter_id on participant
                    } else where_aggr["$participant." + tuple[0].tableName + "_id$"] =
                        tuple[1].id
                }
                where_links[tuple[0].tableName + "_id"] = tuple[1].get("id");
            });
            return [where_aggr, where_links];
        });
    }

    // create an array with every possible combination
    // hero x game mode x …
    // Vox  x casual    x …
    // SAW  x casual    x …
    // …
    // Vox  x ranked    x …
    // SAW  x ranked    x …
    // …
    // Vox  x ANY       x …
    const player_points = calculate_point(cartesian(player_dimensions)),
        global_points = calculate_point(cartesian(global_dimensions));

    await ch.prefetch(CRUNCHERS);

    ch.consume("crunch", async (msg) => {
        const player_id = msg.content.toString();
        logger.info("working",
            { type: msg.properties.type, id: player_id });

        let profiler = logger.startTimer();
        // service wide stats
        if (msg.properties.type == "global") {
            try {
                await calculate_global_point();
                logger.info("acking");
                await ch.ack(msg);
                // tell web
                await ch.publish("amq.topic", "global", new Buffer("points_update"));
            } catch (err) {
                logger.error("SQL error", err);
                await ch.nack(msg, false, true);  // requeue
            }
        }
        // player stats
        if (msg.properties.type == "player") {
            try {
                await calculate_player_point(player_id);
                logger.info("acking");
                await ch.ack(msg);
            } catch (err) {
                logger.error("SQL error", err);
                await ch.nack(msg, false, true);  // requeue
            }
            // tell web
            const player = await model.Player.findOne({
                where: { api_id: player_id },
                attributes: ["name"]
            });
            if (player != null) {
                logger.info("updated player", { name: player.get("name") });
                await ch.publish("amq.topic", "player." + player.get("name"),
                    new Buffer("points_update"));
            }
        }
        profiler.done("calculations for " +
            msg.properties.type + " " + player_id);
    }, { noAck: false });

    async function calculate_global_point() {
        logger.info("crunching global stats, this could take a while");
        await Promise.map(global_points, async (tuple, idx, len) => {
            const progress = Math.floor(100*100 * (1-idx/len)) / 100,
                where_aggr = tuple[0], where_links = tuple[1];
            // aggregate participant_stats with our condition
            let stats = await aggregate_stats(where_aggr);
            stats.updated_at = seq.fn("NOW");
            logger.info("inserting global stat", { progress: progress });
            Object.assign(stats, where_links);
            await model.GlobalPoint.upsert(stats);
        }, { concurrency: MAXCONNS });
        logger.info("committing");
    }

    async function calculate_player_point(player_api_id) {
        let point_records = [];
        logger.info("crunching player", { id: player_api_id });

        await Promise.map(player_points, async (tuple) => {
            const where_aggr = tuple[0],
                where_links = tuple[1];
            // make it player specific
            where_aggr["$participant.player_api_id$"] = player_api_id;
            where_aggr["final"] = true;  // only end of match stats
            where_links["player_api_id"] = player_api_id;
            // aggregate participant_stats with our condition
            let stats = await aggregate_stats(where_aggr);
            if (stats != undefined) {
                stats.updated_at = seq.fn("NOW");
                Object.assign(stats, where_links);
                point_records.push(stats);
            }
        }, { concurrency: MAXCONNS });

        logger.info("inserting into db");
        await seq.transaction({ autocommit: false }, async (transaction) => {
            await Promise.map(chunks(point_records), async (p_r) =>
                model.PlayerPoint.bulkCreate(p_r, {
                    updateOnDuplicate: [],  // all
                    transaction: transaction
                }), { concurrency: MAXCONNS }
            )
        });
    }

    // return aggregated stats based on $where as WHERE clauses
    async function aggregate_stats(where) {
        // in literals: q -> column name, e -> function or string
        const q = (qry) => seq.dialect.QueryGenerator.quote(qry),
            e = (qry) => seq.dialect.QueryGenerator.escape(qry);

        // alternative for win rate
        //[ seq.literal(`${e(seq.fn("sum", seq.cast(seq.col("participant.winner"), "int") ))} / ${e(seq.fn("count", seq.col("participant.id")))}`), "win_rate" ]

        // short to sum a participant row as player stat with the same name
        const sum = (name) => [ seq.fn("sum", seq.col("participant_stats." + name)), name ];

        const data = await model.ParticipantStats.findOne({
            where: where,
            attributes: [
                [ seq.fn("count", seq.col("participant.id")), "played" ],
                [ seq.fn("sum", seq.col("duration")), "time_spent" ],
                [ seq.fn("sum", seq.cast(seq.col("participant.winner"), "int") ), "wins" ],
                sum("kills"),
                sum("deaths"),
                sum("assists"),
                sum("minion_kills"),
                sum("jungle_kills"),
                sum("non_jungle_minion_kills"),
                sum("crystal_mine_captures"),
                sum("turret_captures"),
                sum("kda_ratio"),
                sum("kill_participation"),
                sum("impact_score"),
                sum("objective_score"),
                sum("damage_cp_score"),
                sum("damage_wp_score"),
                sum("sustain_score"),
                sum("farm_lane_score"),
                sum("kill_score"),
                sum("objective_lane_score"),
                sum("farm_jungle_score"),
                sum("peel_score"),
                sum("kill_assist_score"),
                sum("objective_jungle_score"),
                sum("vision_score"),
                sum("heal_score"),
                sum("assist_score"),
                sum("utility_score"),
                sum("synergy_score"),
                sum("build_score"),
                sum("offmeta_score"),
                sum("kraken_captures"),
                sum("gold")
            ],
            include: [ {
                model: model.Participant,
                as: "participant",
                attributes: []
            } ]
        });
        return data.dataValues;
    }
})();
