#!/usr/bin/python3

import asyncio
import os
import glob
import logging
import json
import asyncpg

import joblib.worker

queue_db = {
    "host": os.environ.get("POSTGRESQL_SOURCE_HOST") or "localhost",
    "port": os.environ.get("POSTGRESQL_SOURCE_PORT") or 5433,
    "user": os.environ.get("POSTGRESQL_SOURCE_USER") or "vainraw",
    "password": os.environ.get("POSTGRESQL_SOURCE_PASSWORD") or "vainraw",
    "database": os.environ.get("POSTGRESQL_SOURCE_DB") or "vainsocial-raw"
}

db_config = {
    "host": os.environ.get("POSTGRESQL_DEST_HOST") or "localhost",
    "port": os.environ.get("POSTGRESQL_DEST_PORT") or 5432,
    "user": os.environ.get("POSTGRESQL_DEST_USER") or "vainweb",
    "password": os.environ.get("POSTGRESQL_DEST_PASSWORD") or "vainweb",
    "database": os.environ.get("POSTGRESQL_DEST_DB") or "vainsocial-web"
}


class Cruncher(joblib.worker.Worker):
    def __init__(self):
        self._con = None
        super().__init__(jobtype="crunch")

    async def connect(self, dbconf, queuedb):
        """Connect to database."""
        logging.warning("connecting to database")
        await super().connect(**queuedb)
        self._con = await asyncpg.connect(**dbconf)

    async def setup(self):
        pass

    async def _windup(self):
        self._tr = self._con.transaction()
        await self._tr.start()

    async def _teardown(self, failed):
        if failed:
            await self._tr.rollback()
        else:
            await self._tr.commit()

    async def _execute_job(self, jobid, payload, priority):
        """Finish a job."""
        dimension_id = payload["dimension"]

        logging.debug("%s: crunching dimension %s",
                      jobid, dimension_id)

        dimension = await self._con.fetchrow("""
        SELECT * FROM stats_dimensions WHERE id=$1
        """, dimension_id)
        if dimension is None:
            logging.warning("%s: invalid dimension '%s', exiting",
                            jobid, dimension_id)
            raise joblib.worker.JobFailed("invalid dimension '" + dimension_id
                                          + "'",
                                          False)

        table = dimension["dimension_on"]
        field = dimension["name"]
        value = dimension["value"]

        if table == "hero":
            filter_query = ""
            if field == "patch":
                filter_query = "WHERE match.patch_version='" + value + "'"
            if field == "recent_matches":
                filter_query = "WHERE match.api_id IN (SELECT api_id FROM match ORDER BY created_at DESC LIMIT " + value + ")"
            if field == "skill_tier":
                filter_query = "WHERE participant.skill_tier=" + value

            stats = await self._con.fetch("""
            SELECT
                heros.id,
                SUM(participant.winner::INT)/COUNT(participant.winner)::FLOAT AS win_rate,
                COUNT(participant.hero)/(SELECT COUNT(hero) FROM participant)::FLOAT AS pick_rate,
                SUM(60*participant.minion_kills/match.duration::FLOAT)/COUNT(participant.farm)::FLOAT AS cs_per_min,
                0 AS gold_per_min
            FROM participant

            JOIN participant_stats ON participant.api_id=participant_stats.participant_api_id
            JOIN roster ON roster.api_id=participant.roster_api_id
            JOIN match ON match.api_id=roster.match_api_id

            JOIN heros ON heros.api_name=hero
            """
            + filter_query +
            """
            GROUP BY heros.id
            """)

            for stat in stats:
                stat_id = await self._con.fetchrow("""
                    INSERT INTO stats(win_rate, pick_rate, cs_per_min, gold_per_min)
                    VALUES($1, $2, $3)
                    RETURNING stats.id
                    """, stat["win_rate"], stat["pick_rate"], stat["cs_per_min"],
                    stat["gold_per_min"])
                await self._con.fetch("""
                    INSERT INTO hero_stats(hero_id, dimension_id, stats_id, computed_on)
                    VALUES($1, $2, $3, NOW())
                    """, stat["id"], dimension["id"],
                    stat_id["id"])

async def startup():
    worker = Cruncher()
    await worker.connect(db_config, queue_db)
    await worker.setup()
    await worker.start(batchlimit=1)


logging.basicConfig(level=logging.DEBUG)

loop = asyncio.get_event_loop()
loop.run_until_complete(startup())
loop.run_forever()
