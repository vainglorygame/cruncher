#!/usr/bin/node
/* jshint esnext:true */
"use strict";

const amqp = require("amqplib"),
    Promise = require("bluebird"),
    winston = require("winston"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise"),
    hash = require("object-hash");

const RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    // number of inserts in one statement
    CHUNKSIZE = parseInt(process.env.CHUNKSIZE) || 100,
    CRUNCHERS = process.env.CRUNCHERS || 4;  // how many players to crunch concurrently

const logger = new (winston.Logger)({
        transports: [
            new (winston.transports.Console)({
                timestamp: () => Date.now(),
                formatter: (options) => winston.config.colorize(options.level,
`${new Date(options.timestamp()).toISOString()} ${options.level.toUpperCase()} ${(options.message? options.message:"")} ${(options.meta && Object.keys(options.meta).length? JSON.stringify(options.meta):"")}`)
            })
        ]
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
            seq = new Seq(DATABASE_URI, { logging: false }),
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
    async function dimensions_for(dimensions) {
        let cache = [];
        await Promise.all(dimensions.map(async (d, idx) =>
            cache[idx] = (await d.findAll()).map((o) => [d, o]))
        );
        return cache;
    }
    const player_dimensions = await dimensions_for(
        [model.Series, model.Filter, model.Hero, model.Role,
            model.GameMode]),
        global_dimensions = await dimensions_for(
        [model.Series, model.Filter, model.Hero, model.Role,
            model.GameMode, model.Skilltier, model.Build]);

    // return every possible [query, insert] combination
    function calculate_point(points, instance) {
        return points.map((point) => {
            // Series and Filter are special
            let where_aggr = {},
                where_links = {};
            // create skeleton: where hero_id=$hero
            // for aggregation, use series as range
            // for links, use the id
            point.forEach((tuple) => {
                // [table name, table element]
                if (tuple[1].get("dimension_on") != null &&
                    tuple[1].get("dimension_on") != instance) return;
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
                    } else where_aggr["$participant." + tuple[0].tableName + ".id$"] =
                        tuple[1].id
                }
                where_links[tuple[0].tableName + "_id"] = tuple[1].id
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
    const player_points = calculate_point(cartesian(player_dimensions),
            "player"),
        global_points = calculate_point(cartesian(global_dimensions),
            "global");

    await ch.prefetch(CRUNCHERS);

    ch.consume("crunch", async (msg) => {
        let player_records = [],
            global_records = [],
            player_id = msg.content.toString();

        logger.info("working for %s on %s",
            msg.properties.type, player_id);

        let calculation_profiler = logger.startTimer();
        if (msg.properties.type == "global") {
            const records = await calculate_global_point();
            if (records != undefined)
                global_records = global_records.concat(records);
        }
        if (msg.properties.type == "player") {
            const records = await calculate_player_point(player_id);
            if (records != undefined)
                player_records = player_records.concat(records);
        }
        calculation_profiler.done("calculations for " +
            msg.properties.type + " " + player_id);

        let transaction_profiler = logger.startTimer();
        try {
            logger.info("inserting into db");
            await seq.transaction({ autocommit: false }, (transaction) => {
                return Promise.all([
                    Promise.map(chunks(player_records), async (p_r) =>
                        model.PlayerPoint.bulkCreate(p_r, {
                            updateOnDuplicate: [],  // all
                            transaction: transaction
                        })
                    ),
                    Promise.map(chunks(global_records), async (g_r) =>
                        model.GlobalPoint.bulkCreate(g_r, {
                            updateOnDuplicate: [],
                            transaction: transaction
                        })
                    )
                ]);
            });
            logger.info("acking");
            await ch.ack(msg);
        } catch (err) {
            // TODO
            logger.error("SQL error: %s, %j, %s",
                err.name, err.errors, err.parent);
            await ch.nack(msg, false, true);  // requeue
        }
        transaction_profiler.done("database transaction");

        if (player_records.length > 0) {
            const player = await model.Player.findOne({
                where: { api_id: player_id },
                attributes: ["name"]
            });
            if (player != null) {
                logger.info("updated player '%s'", player.get("name"));
                await ch.publish("amq.topic", "player." + player.get("name"),
                    new Buffer("points_update"));
            }
        }
        if (global_records.length > 0)
            await ch.publish("amq.topic", "global", new Buffer("points_update"));
    }, { noAck: false });

    async function calculate_global_point() {
        let global_records = [];
        logger.info("crunching global stats, this could take a while");

        await Promise.all(global_points.map(async (tuple) => {
            const where_aggr = tuple[0],
                where_links = tuple[1];
            // aggregate participant_stats with our condition
            let stats = await aggregate_stats(where_aggr);
            if (stats != undefined) {
                stats.updated_at = seq.fn("NOW");
                logger.info("inserting global stats");
                Object.assign(stats, where_links);
                global_records.push(stats);
            } else logger.warn("not enough data for this global stat!");
        }));
        return global_records;
    }

    async function calculate_player_point(player_api_id) {
        let point_records = [];
        logger.info("crunching player %s", player_api_id);

        await Promise.all(player_points.map(async (tuple) => {
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
        }));
        return point_records;
    }

    // return aggregated stats based on $where as WHERE clauses
    async function aggregate_stats(where) {
        // in literals: q -> column name, e -> function or string
        const q = (qry) => seq.dialect.QueryGenerator.quote(qry),
            e = (qry) => seq.dialect.QueryGenerator.escape(qry);

        // alternative for win rate
        //[ seq.literal(`${e(seq.fn("sum", seq.cast(seq.col("participant.winner"), "int") ))} / ${e(seq.fn("count", seq.col("participant.id")))}`), "win_rate" ]

        const associations = [ {
            model: model.Participant,
            as: "participant",
            attributes: [],
            include: [ {
                model: model.Roster,
                attributes: [],
                include: [ {
                    model: model.Match,
                    attributes: []
                } ]
                }, {
                    model: model.Hero,
                    as: "hero",
                    attributes: []
                }, {
                    model: model.Series,
                    as: "series",
                    attributes: []
                }, {
                    model: model.GameMode,
                    as: "game_mode",
                    attributes: []
                }, {
                    model: model.Role,
                    as: "role",
                    attributes: []
            } ]
        } ];

        const played = await model.ParticipantStats.count({
            where: where,
            include: associations
        });
        if (played == 0) return undefined;  // not enough data

        // short to sum a participant row as player stat with the same name
        const sum = (name) => [ seq.fn("sum", seq.col("participant_stats." + name)), name ];

        const data = await model.ParticipantStats.findOne({
            where: where,
            attributes: [
                [ seq.fn("count", seq.col("participant.id")), "played" ],
                [ seq.fn("sum", seq.col("participant.roster.match.duration")), "time_spent" ],
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
            include: associations
        });
        return data.dataValues;
    }
})();
