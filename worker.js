#!/usr/bin/node
/* jshint esnext:true */
"use strict";

var amqp = require("amqplib"),
    Seq = require("sequelize"),
    sleep = require("sleep-promise"),
    hash = require("object-hash");

var RABBITMQ_URI = process.env.RABBITMQ_URI,
    DATABASE_URI = process.env.DATABASE_URI;

(async () => {
    let seq, model, rabbit, ch;

    while (true) {
        try {
            seq = new Seq(DATABASE_URI),
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

    let queue = [],
        timer = undefined;

    await seq.sync();

    // no batching for cruncher, not worth it
    // we'll read a million tables and commit just one record
    // once per hour
    await ch.prefetch(1);

    ch.consume("crunch", async (msg) => {
        let dim = JSON.parse(msg.content),
            filter = dim.filter, dimension, filter_hash,
            action;
        console.log("crunching dimension", dim);

        // we don't want to store duplicate filters
        // but we can't reliably compare JSON in SQL either
        // so we hash the object and lookup/store a sha1
        filter_hash = hash(dim.filter, { unorderedArrays: true });
        try {
            dimension = (await model.StatsDimensions.findOrCreate({
                where: {
                    dimension_on: dim.dimension_on,
                    filter_hash: filter_hash
                }
            }))[0];
        } catch (err) {
            console.error(err);
            await ch.nack(msg, false, false);
            return;
        }

        // not implemented -> error
        switch (dimension.dimension_on) {
            case "hero":
                action = calculateHeroStats(dimension.id, filter);
                break;
            default:
                action = async () => { throw "dimension_on not implemented " + dimension.dimension_on; };
        }

        try {
            await action;
            console.log("acking");
            await ch.ack(msg);
        } catch (err) {
            console.error(err);
            await ch.nack(msg, false, false);  // nack and do not requeue
        }
    }, { noAck: false });

    async function calculateHeroStats(dimension_id, filter) {
        let win_rate, pick_rate, gold_per_min;

        // in literals: q -> column name, e -> function or string
        let q = (qry) => seq.dialect.QueryGenerator.quote(qry),
            e = (qry) => seq.dialect.QueryGenerator.escape(qry);

        // TODO (workaround) ignore old API data where participant.gold was null
        filter["$participant.gold$"] = { $ne: null };
        console.log(filter);

        // alternative for win rate
        //[ seq.literal(`${e(seq.fn("sum", seq.cast(seq.col("participant.winner"), "int") ))} / ${e(seq.fn("count", seq.col("participant.id")))}`), "win_rate" ]

        // calculate stats
        let total_participants = await model.Participant.count({
            where: filter,
            include: [ {
                model: model.Roster,
                attributes: [],
                include: [ {
                    model: model.Match,
                    attributes: []
                } ]
            }, {
                model: model.Heros,
                attributes: []
            } ]
        });

        // TODO (workaround) filter unmapped actor<->hero
        filter["$hero.id$"] = { $ne: null };
        let data = await model.Participant.findAll({
            where: filter,
            attributes: [
                // meta
                [ seq.col("hero.name"), "hero_name" ],
                [ seq.col("hero.id"), "hero_id" ],

                // stats, see `hero_stats` table
                [ seq.literal(`${q(seq.fn("count", seq.col("participant.id") ))} / ${total_participants}`), "pick_rate" ],
                [ seq.fn("avg", seq.cast(seq.col("participant.winner"), "int") ), "win_rate" ],
                [ seq.fn("sum", seq.literal(`${q("participant.gold")} / ${q("roster.match.duration")}`)), "gold_per_min" ],
                [ seq.fn("avg", seq.literal(`60.0 * ${q("participant.minion_kills")} / ${q("roster.match.duration")}`)), "cs_per_min" ]
            ],
            group: q("hero.name"),
            include: [ {
                model: model.Roster,
                attributes: [],
                include: [ {
                    model: model.Match,
                    attributes: []
                } ]
            }, {
                model: model.Heros,
                attributes: []
            } ]
        });
        console.log("hero stats", filter, data.map((d) => d.dataValues));
        // insert hero x dimension x stats
        await seq.transaction({ autocommit: false }, (transaction) =>
            Promise.all(data.map(async (record) => {
                let hero_stat_db = await model.HeroStats.create(record.dataValues);
                await model.HeroDimension.create({
                    hero_id: record.get("hero_id"),
                    dimension_id: dimension_id,
                    stats_id: hero_stat_db.get("id"),
                    computed_on: new Date()
                }, { transaction: transaction });
            }))
        );
    }
})();
