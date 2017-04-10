#!/usr/bin/node
/* jshint esnext:true */
"use strict";

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise"),
    hash = require("object-hash");

var RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 200,  // players + globals
    IDLE_TIMEOUT = parseFloat(process.env.IDLE_TIMEOUT) || 1000;  // ms

(async () => {
    let seq, model, rabbit, ch;

    while (true) {
        try {
            seq = new Seq(DATABASE_URI, { logging: false }),
            rabbit = await amqp.connect(RABBITMQ_URI),
            ch = await rabbit.createChannel();
            await ch.assertQueue("crunch", {durable: true});
            break;
        } catch (err) {
            console.error(err);
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

    let dimension_cache = [];
    // create a 3D array
    // [[ ["hero", "Vox"], ["hero", "Taka"], …], [ ["game_mode", "ranked"], … ], …]
    // (will not use "Vox" but the index instead)
    await Promise.all(
        [model.Series, model.Filter, model.Hero,
            model.GameMode].map(async (d, idx) =>
            dimension_cache[idx] =
                (await d.findAll()).map((o) => [d, o]))
    );

    // create an array with every possible combination
    // hero x game mode x …
    // Vox  x casual    x …
    // SAW  x casual    x …
    // …
    // Vox  x ranked    x …
    // SAW  x ranked    x …
    // …
    // Vox  x ANY       x …
    let points = cartesian(dimension_cache);

    // return every possible [query, insert] combination
    function calculate_point(dimensions) {
        return points.map((point) => {
            // Series and Filter are special
            let where_aggr = {},
                where_links = {};
            // create skeleton: where hero_id=$hero
            // for aggregation, use series as range
            // for links, use the id
            point.forEach((tuple) => {
                // [table name, table element]
                if (dimensions.indexOf(tuple[0].tableName) > -1) {
                    if (tuple[1].get("name") != "all") {
                        // exclude series & filter, added below
                        if (tuple[0].tableName == "filter") {
                            // merge custom filters
                            if (tuple[1].get("dimension_on") == "player")
                                Object.assign(where_aggr,
                                    tuple[1].get("filter"));
                        } else if (tuple[0].tableName == "series") {
                            if (tuple[1].get("dimension_on") == "player")
                                // series is special,
                                // use start < date < end comparison
                                where_aggr.created_at = { $between: [
                                    tuple[1].get("start"),
                                    tuple[1].get("end")
                                ] }
                        } else where_aggr["$participant." + tuple[0].tableName + ".id$"] =
                            tuple[1].id
                    }
                    where_links[tuple[0].tableName + "_id"] = tuple[1].id
                }
            });
            return [where_aggr, where_links];
        });
    }
    let player_points = calculate_point(["series", "filter", "hero", "game_mode"]),
        global_points = calculate_point(["series", "filter", "hero", "game_mode"]);  // TODO

    let queue = [],
        timer = undefined;

    await seq.sync();

    await ch.prefetch(BATCHSIZE);

    ch.consume("crunch", (msg) => {
        queue.push(msg);

        // fill queue until batchsize or idle
        if (timer != undefined) clearTimeout(timer);
        timer = setTimeout(process, IDLE_TIMEOUT);
        if (queue.length == BATCHSIZE)
            process();
    }, { noAck: false });

    async function process() {
        console.log("processing batch", queue.length);

        // clean up to allow processor to accept while we wait for db
        clearTimeout(timer);
        timer = undefined;
        let msgs = queue;
        queue = [];

        let player_records = [],
            global_records = [];

        await Promise.all(msgs.map(async (msg) => {
            if (msg.properties.type == "global") {
                // TODO
                let stats = await calculate_global_point();
                if (stats != undefined) global_records.push(stats);
            }
            if (msg.properties.type == "player") {
                let stats = await calculate_player_point(
                    msg.content.toString());
                if (stats != undefined) player_records.push(stats);
            }
        }));

        try {
            await seq.transaction({ autocommit: false }, async (transaction) => {
                console.log("inserting batch into db");
                await Promise.all([
                    model.PlayerPoint.bulkCreate(player_records, {
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    }),
                    model.GlobalPoint.bulkCreate(global_records, {
                        updateOnDuplicate: [],  // all
                        transaction: transaction
                    })
                ]);
            });
            console.log("acking batch");
            await Promise.all(msgs.map((m) => ch.ack(m)) );
        } catch (err) {
            // TODO
            console.error(err);
            await Promise.all(msgs.map((m) => ch.nack(m, true)) );  // requeue
        }
    }

    async function calculate_global_point() {
        console.log("crunching global stats, this could take a while");

        await Promise.all(global_points.map(async (tuple) => {
            let where_aggr = tuple[0],
                where_links = tuple[1];
            // aggregate participant_stats with our condition
            let stats = await aggregate_stats(where_aggr);
            if (stats != undefined) {
                console.log("inserting global stats");
                Object.assign(stats, where_links);
                return stats;
            }
            return undefined;
        }));
    }

    async function calculate_player_point(player_api_id) {
        let player = await model.Player.findOne(
            { where: { api_id: player_api_id } });
        if (player == null) {
            console.error("player does not exist in db!?");
            return;
        }
        if (player.get("last_update") == null)
            // not enough data
            return;
        console.log("crunching player", player.name);

        await Promise.all(player_points.map(async (tuple) => {
            let where_aggr = tuple[0],
                where_links = tuple[1];
            // make it player specific
            where_aggr["$participant.player_api_id$"] = player_api_id;
            where_links["player_id"] = player.id;
            // aggregate participant_stats with our condition
            let stats = await aggregate_stats(where_aggr);
            if (stats != undefined) {
                console.log("inserting stats for player", player.name);
                Object.assign(stats, where_links);
                await model.PlayerPoint.upsert(stats);
            }
        }));
    }

    // return aggregated stats based on $where as WHERE clauses
    async function aggregate_stats(where) {
        // in literals: q -> column name, e -> function or string
        let q = (qry) => seq.dialect.QueryGenerator.quote(qry),
            e = (qry) => seq.dialect.QueryGenerator.escape(qry);

        // alternative for win rate
        //[ seq.literal(`${e(seq.fn("sum", seq.cast(seq.col("participant.winner"), "int") ))} / ${e(seq.fn("count", seq.col("participant.id")))}`), "win_rate" ]

        let associations = [ {
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

        let played = await model.ParticipantStats.count({
            where: where,
            include: associations
        });
        if (played == 0) return undefined;  // not enough data

        // short to sum a participant row as player stat with the same name
        let sum = (name) => [ seq.fn("sum", seq.col("participant_stats." + name)), name ];

        let data = await model.ParticipantStats.findOne({
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
                sum("cs_per_min"),
                sum("kills_per_min"),
                sum("impact_score"),
                sum("objective_score"),
                sum("damage_cp_score"),
                sum("damage_wp_score"),
                sum("sustain_score"),
                sum("farm_lane_score"),
                sum("kill_score"),
                sum("objective_lane_score"),
                sum("farm_jungle_score"),
                sum("peal_score"),
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
