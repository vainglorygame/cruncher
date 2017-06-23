INSERT LOW_PRIORITY INTO global_point
SELECT
    NULL,
    NOW(),
    `series`.`id`,
    `filter`.`id`,
    `player_hero`.`id`,
    `game_mode`.`id`,
    `skill_tier`.`id`,
    `build`.`id`,
    `player_role`.`id`,
    `enemy_hero`.`id`,
    `enemy_role`.`id`,
    `region`.`id`,

    -- everything added here needs to be in ON DUPLICATE too!!!
    COUNT(`player`.`id`) AS `played`,
    SUM(CAST(`player`.`winner` AS INT)) AS `wins`,
    SUM(`participant_stats`.`duration`) AS `time_spent`,
    SUM(`participant_stats`.`kills`) AS `kills`,
    SUM(`participant_stats`.`deaths`) AS `deaths`,
    SUM(`participant_stats`.`assists`) AS `assists`,
    SUM(`participant_stats`.`farm`) AS `farm`,
    SUM(`participant_stats`.`minion_kills`) AS `minion_kills`,
    SUM(`participant_stats`.`jungle_kills`) AS `jungle_kills`,
    SUM(`participant_stats`.`non_jungle_minion_kills`) AS `non_jungle_minion_kills`,
    SUM(`participant_stats`.`crystal_mine_captures`) AS `crystal_mine_captures`,
    SUM(`participant_stats`.`gold_mine_captures`) AS `gold_mine_captures`,
    SUM(`participant_stats`.`kraken_captures`) AS `kraken_captures`,
    SUM(`participant_stats`.`turret_captures`) AS `turret_captures`,
    SUM(`participant_stats`.`gold`) AS `gold`,
    SUM(`participant_stats`.`impact_score`) AS `impact_score`
FROM `participant` `player`
JOIN `participant_stats` ON `participant_stats`.`participant_api_id` = `player`.`api_id`

-- dimensions
JOIN `series` ON `participant_stats`.`created_at` BETWEEN `series`.`start` AND `series`.`end` AND `series`.`dimension_on` = 'global'
JOIN `hero` `player_hero` ON `player_hero`.`id` = `player`.`hero_id` OR `player_hero`.`name` = 'all'
JOIN `role` `player_role` ON `player_role`.`id` = `player`.`role_id` OR `player_role`.`name` = 'all'
JOIN `region` ON `region`.`name` = `player`.`shard_id` OR `region`.`name` = 'all'

-- filters
JOIN `global_point_filters` ON `player`.`match_api_id` = `global_point_filters`.`match_api_id`
JOIN `filter` ON (`filter`.`name` = 'all' AND `filter`.`dimension_on` = 'global') OR `global_point_filters`.`filter_id` = `filter`.`id`

-- being cheap
JOIN `game_mode` ON `game_mode`.`id` = `player`.`game_mode_id` OR `game_mode`.`name` = 'all'
    AND `series`.`show_in_web` = TRUE
JOIN `skill_tier` ON `player`.`skill_tier` BETWEEN `skill_tier`.`start` AND `skill_tier`.`end` OR `skill_tier`.`name` = 'all'
    AND `series`.`show_in_web` = TRUE

-- builds and counters do not cross 
-- builds
JOIN `build` ON `build`.`name` = 'all' OR (
    -- do not cross daily series, game mode, skill tier, region (builds excluded below)
    `series`.`show_in_web` = TRUE AND
    `game_mode`.`name` = 'all' AND
    `skill_tier`.`name` = 'all' AND
    `region`.`name` = 'all' AND

    `build`.`item_1` IS NOT NULL OR `participant_stats`.`item_grants` RLIKE CONCAT(:build_regex_start, `build`.`item_1`, ';', `build`.`item_1_count`, :build_regex_end) AND
    `build`.`item_2` IS NOT NULL OR `participant_stats`.`item_grants` RLIKE CONCAT(:build_regex_start, `build`.`item_2`, ';', `build`.`item_2_count`, :build_regex_end) AND
    `build`.`item_3` IS NOT NULL OR `participant_stats`.`item_grants` RLIKE CONCAT(:build_regex_start, `build`.`item_3`, ';', `build`.`item_3_count`, :build_regex_end) AND
    `build`.`item_4` IS NOT NULL OR `participant_stats`.`item_grants` RLIKE CONCAT(:build_regex_start, `build`.`item_4`, ';', `build`.`item_4_count`, :build_regex_end) AND
    `build`.`item_5` IS NOT NULL OR `participant_stats`.`item_grants` RLIKE CONCAT(:build_regex_start, `build`.`item_5`, ';', `build`.`item_5_count`, :build_regex_end) AND
    `build`.`item_6` IS NOT NULL OR `participant_stats`.`item_grants` RLIKE CONCAT(:build_regex_start, `build`.`item_6`, ';', `build`.`item_6_count`, :build_regex_end)
)

-- counters
JOIN `participant` `enemy` ON `enemy`.`match_api_id` = `player`.`match_api_id` AND `enemy`.`winner` <> `player`.`winner`
    -- do not cross daily series, builds, game mode, skill tier, region
    AND `series`.`show_in_web` = TRUE
    AND `build`.`name` = 'all'
    AND `game_mode`.`name` = 'all'
    AND `skill_tier`.`name` = 'all'
    AND `region`.`name` = 'all'
JOIN `hero` `enemy_hero` ON `enemy_hero`.`id` = `enemy`.`hero_id` OR `enemy_hero`.`name` = 'all'
JOIN `role` `enemy_role` ON `enemy_role`.`id` = `enemy`.`role_id` OR `enemy_role`.`name` = 'all'

WHERE `player`.`api_id` IN (:participant_api_ids)

GROUP BY `series`.`id`, `filter`.`id`, `player_hero`.`id`, `game_mode`.`id`, `skill_tier`.`id`, `build`.`id`, `player_role`.`id`, `enemy_hero`.`id`, `enemy_role`.`id`, `region`.`id`
ORDER BY `player`.`id`

ON DUPLICATE KEY UPDATE
`played` = `played` + VALUES(`played`),
`wins` = `wins` + VALUES(`wins`),
`time_spent` = `time_spent` + VALUES(`time_spent`),
`kills` = `kills` + VALUES(`kills`),
`deaths` = `deaths` + VALUES(`deaths`),
`assists` = `assists` + VALUES(`assists`),
`farm` = `farm` + VALUES(`farm`),
`minion_kills` = `minion_kills` + VALUES(`minion_kills`),
`jungle_kills` = `jungle_kills` + VALUES(`jungle_kills`),
`non_jungle_minion_kills` = `non_jungle_minion_kills` + VALUES(`non_jungle_minion_kills`),
`crystal_mine_captures` = `crystal_mine_captures` + VALUES(`crystal_mine_captures`),
`gold_mine_captures` = `gold_mine_captures` + VALUES(`gold_mine_captures`),
`kraken_captures` = `kraken_captures` + VALUES(`kraken_captures`),
`turret_captures` = `turret_captures` + VALUES(`turret_captures`),
`gold` = `gold` + VALUES(`gold`),
`impact_score` = `impact_score` + VALUES(`impact_score`)
