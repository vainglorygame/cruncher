#!/usr/bin/node
/* jshint esnext:true */
"use strict";

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise"),
    hash = require("object-hash");

var RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI,
    BATCHSIZE = parseInt(process.env.BATCHSIZE) || 5,  // players + globals
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

    await seq.sync();

    await ch.prefetch(1);  // TODO batching

    ch.consume("crunch", async (msg) => {
        if (msg.properties.type == "global") {}

        try {
            if (msg.properties.type == "player")
                await calculate_player_point(msg.content.toString());
            console.log("acking");
            await ch.ack(msg);
        } catch (err) {
            console.error(err);
            await ch.nack(msg, false, false);  // nack and do not requeue
        }
    }, { noAck: false });

    async function calculate_player_point(player_api_id) {
        let win_rate, pick_rate, gold_per_min;

        let player = await model.Player.findOne(
            { where: { api_id: player_api_id } });
        // TODO cache dimensions
        // Series and Filter are special
        let dimensions = [model.Series, model.Filter, model.Hero, model.GameMode],
            player_dimensions = ["series", "filter", "hero", "game_mode"],
            dimension_cache = [];
        // create a 3D array
        // [[ ["hero", "Vox"], ["hero", "Taka"], …], [ ["game_mode", "ranked"], … ], …]
        // (will not use "Vox" but the index instead)
        await Promise.all(
            dimensions.map(async (d, idx) =>
                dimension_cache[idx] = 
                    (await d.findAll()).map((o) => [d, o]))
        );

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
        // create an array with every possible combination
        // hero x game mode x …
        // Vox  x casual    x …
        // SAW  x casual    x …
        // …
        // Vox  x ranked    x …
        // SAW  x ranked    x …
        // …
        // Vox  x ANY       x …
        let combos = cartesian(dimension_cache);
        await Promise.all(combos.map(async (combo) => {
            let where_aggr = {},
                where_links = {};
            // create skeleton: where hero_id=$hero
            // for aggregation, use series as range
            // for links, use the id
            combo.map((tuple) => {
                // [table name, table element]
                if (player_dimensions.indexOf(tuple[0].tableName) > -1) {
                    if (tuple[1].get("name") != "all") {
                        // exclude series & filter, added below
                        if (tuple[0].tableName == "filter") {
                            // merge custom filters
                            if (tuple[1].get("filter_on") == "player")
                                Object.assign(where_aggr,
                                    tuple[1].get("filter"));
                        } else if (tuple[0].tableName == "series") {
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
            // make it player specific
            where_aggr["$participant.player_api_id$"] = player_api_id;
            where_links["player_id"] = player.id;
            // aggregate participant_stats with our condition
            let stats = await aggregate_stats(where_aggr);
            if (stats != undefined) {
                console.log("inserting stats for player", player.name);
                Object.assign(stats, where_links);
                await model.PlayerPoint.upsert(stats, {
                    where: where_links
                });
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

        // TODO cache this?
        let total_participants = await model.ParticipantStats.count({
            where: where,
            include: associations
        });
        if (total_participants == 0) return undefined;  // not enough data

        let data = await model.ParticipantStats.findOne({
            where: where,
            attributes: [
                [ seq.literal(`${q(seq.fn("count", seq.col("participant.id") ))} / ${total_participants}`), "pick_rate" ],
                [ seq.fn("avg", seq.cast(seq.col("participant.winner"), "int") ), "win_rate" ],
                [ seq.fn("sum", seq.literal(`${q("participant_stats.gold")} / ${q("participant.roster.match.duration")}`)), "gold_per_min" ],
                [ seq.fn("avg", seq.literal(`60.0 * ${q("participant_stats.minion_kills")} / ${q("participant.roster.match.duration")}`)), "cs_per_min" ]
            ],
            include: associations
        });
        return data.dataValues;
    }
})();
