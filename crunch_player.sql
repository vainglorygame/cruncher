INSERT LOW_PRIORITY INTO player_point
SELECT
    NULL,
    `participant`.`player_api_id`,

    COUNT(`participant`.`id`) AS `played`,
    SUM(CAST(`participant`.`winner` AS INT)) AS `wins`,
    SUM(`duration`) AS `time_spent`,
    
    `series`.`id`,
    `role`.`id`,
    `filter`.`id`,
    `hero`.`id`,
    `game_mode`.`id`,
    `participant`.`created_at`,
    
    SUM(`participant_stats`.`kills`) AS `kills`,
    SUM(`participant_stats`.`deaths`) AS `deaths`,
    SUM(`participant_stats`.`assists`) AS `assists`,
    SUM(`participant_stats`.`minion_kills`) AS `minion_kills`,
    SUM(`participant_stats`.`jungle_kills`) AS `jungle_kills`,
    SUM(`participant_stats`.`non_jungle_minion_kills`) AS `non_jungle_minion_kills`,
    SUM(`participant_stats`.`crystal_mine_captures`) AS `crystal_mine_captures`,
    SUM(`participant_stats`.`gold_mine_captures`) AS `gold_mine_captures`,
    SUM(`participant_stats`.`kraken_captures`) AS `kraken_captures`,
    SUM(`participant_stats`.`turret_captures`) AS `turret_captures`,
    SUM(`participant_stats`.`gold`) AS `gold`,
    SUM(`participant_stats`.`hero_level`) AS `hero_level`,
    SUM(`participant_stats`.`kda_ratio`) AS `kda_ratio`,
    SUM(`participant_stats`.`kill_participation`) AS `kill_participation`,
    SUM(`participant_stats`.`cs_per_min`) AS `cs_per_min`,
    SUM(`participant_stats`.`kills_per_min`) AS `kills_per_min`,
    SUM(`participant_stats`.`impact_score`) AS `impact_score`,
    SUM(`participant_stats`.`objective_score`) AS `objective_score`,
    SUM(`participant_stats`.`damage_cp_score`) AS `damage_cp_score`,
    SUM(`participant_stats`.`damage_wp_score`) AS `damage_wp_score`,
    SUM(`participant_stats`.`sustain_score`) AS `sustain_score`,
    SUM(`participant_stats`.`farm_lane_score`) AS `farm_lane_score`,
    SUM(`participant_stats`.`kill_score`) AS `kill_score`,
    SUM(`participant_stats`.`objective_lane_score`) AS `objective_lane_score`,
    SUM(`participant_stats`.`farm_jungle_score`) AS `farm_jungle_score`,
    SUM(`participant_stats`.`peel_score`) AS `peel_score`,
    SUM(`participant_stats`.`kill_assist_score`) AS `kill_assist_score`,
    SUM(`participant_stats`.`objective_jungle_score`) AS `objective_jungle_score`,
    SUM(`participant_stats`.`vision_score`) AS `vision_score`,
    SUM(`participant_stats`.`heal_score`) AS `heal_score`,
    SUM(`participant_stats`.`assist_score`) AS `assist_score`,
    SUM(`participant_stats`.`utility_score`) AS `utility_score`,
    SUM(`participant_stats`.`synergy_score`) AS `synergy_score`,
    SUM(`participant_stats`.`build_score`) AS `build_score`,
    SUM(`participant_stats`.`offmeta_score`) AS `offmeta_score`
FROM `participant`
JOIN `participant_stats` ON `participant_stats`.`participant_api_id` = `participant`.`api_id`
JOIN `series` ON `participant_stats`.`created_at` BETWEEN `series`.`start` AND `series`.`end` AND `series`.`dimension_on` = 'global'
JOIN `filter` ON `filter`.`name` = 'all' AND `filter`.`dimension_on` = 'global'
JOIN `hero` ON `hero`.`id` = `participant`.`hero_id` OR `hero`.`name` = 'all'
JOIN `game_mode` ON `game_mode`.`id` = `participant`.`game_mode_id` OR `game_mode`.`name` = 'all'
JOIN `skill_tier` ON `participant`.`skill_tier` BETWEEN `skill_tier`.`start` AND `skill_tier`.`end` OR `skill_tier`.`name` = 'all'
JOIN `build` ON `build`.`name` = 'all'
JOIN `role` ON `role`.`id` = `participant`.`role_id` OR `role`.`name` = 'all'
JOIN `region` ON `region`.`name` = `participant`.`shard_id` OR `region`.`name` = 'all'

WHERE `participant`.`api_id` IN (:participant_api_ids)

GROUP BY `series`.`id`, `filter`.`id`, `hero`.`id`, `game_mode`.`id`, `skill_tier`.`id`, `build`.`id`, `role`.`id`, `region`.`id`
ORDER BY `participant`.`id`

ON DUPLICATE KEY UPDATE
`played` = `played` + VALUES(`played`),
`wins` = `wins` + VALUES(`wins`),
`kills` = `kills` + VALUES(`kills`),
`deaths` = `deaths` + VALUES(`deaths`),
`assists` = `assists` + VALUES(`assists`),
`minion_kills` = `minion_kills` + VALUES(`minion_kills`),
`jungle_kills` = `jungle_kills` + VALUES(`jungle_kills`),
`non_jungle_minion_kills` = `non_jungle_minion_kills` + VALUES(`non_jungle_minion_kills`),
`crystal_mine_captures` = `crystal_mine_captures` + VALUES(`crystal_mine_captures`),
`gold_mine_captures` = `gold_mine_captures` + VALUES(`gold_mine_captures`),
`kraken_captures` = `kraken_captures` + VALUES(`kraken_captures`),
`turret_captures` = `turret_captures` + VALUES(`turret_captures`),
`gold` = `gold` + VALUES(`gold`),
`kda_ratio` = `kda_ratio` + VALUES(`kda_ratio`),
`kill_participation` = `kill_participation` + VALUES(`kill_participation`),
`cs_per_min` = `cs_per_min` + VALUES(`cs_per_min`),
`kills_per_min` = `kills_per_min` + VALUES(`kills_per_min`),
`impact_score` = `impact_score` + VALUES(`impact_score`),
`objective_score` = `objective_score` + VALUES(`objective_score`),
`damage_cp_score` = `damage_cp_score` + VALUES(`damage_cp_score`),
`damage_wp_score` = `damage_wp_score` + VALUES(`damage_wp_score`),
`sustain_score` = `sustain_score` + VALUES(`sustain_score`),
`farm_lane_score` = `farm_lane_score` + VALUES(`farm_lane_score`),
`kill_score` = `kill_score` + VALUES(`kill_score`),
`objective_lane_score` = `objective_lane_score` + VALUES(`objective_lane_score`),
`farm_jungle_score` = `farm_jungle_score` + VALUES(`farm_jungle_score`),
`peel_score` = `peel_score` + VALUES(`peel_score`),
`kill_assist_score` = `kill_assist_score` + VALUES(`kill_assist_score`),
`objective_jungle_score` = `objective_jungle_score` + VALUES(`objective_jungle_score`),
`vision_score` = `vision_score` + VALUES(`vision_score`),
`heal_score` = `heal_score` + VALUES(`heal_score`),
`assist_score` = `assist_score` + VALUES(`assist_score`),
`utility_score` = `utility_score` + VALUES(`utility_score`),
`synergy_score` = `synergy_score` + VALUES(`synergy_score`),
`build_score` = `build_score` + VALUES(`build_score`),
`offmeta_score` = `offmeta_score` + VALUES(`offmeta_score`)
