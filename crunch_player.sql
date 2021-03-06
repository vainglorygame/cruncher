insert low_priority into `player_point`
select
    null,
    p.created_at as updated_at,
    p.player_api_id,
    s.id as series_id,
    f.id as filter_id,
    h.id as hero_id,
    gm.id as game_mode_id,
    r.id as role_id,


    -- everything added here needs to be in on DUPLICATE too!!!
    count(p.id) as played,
    sum(cast(p.winner as INT)) as wins,
    sum(p_s.duration) as time_spent,

    sum(p_s.kills) as kills,
    sum(p_s.deaths) as deaths,
    sum(p_s.assists) as assists,
    sum(p_s.farm) as farm,
    sum(p_s.minion_kills) as minion_kills,
    sum(p_s.jungle_kills) as jungle_kills,
    sum(p_s.non_jungle_minion_kills) as non_jungle_minion_kills,
    sum(p_s.crystal_mine_captures) as crystal_mine_captures,
    sum(p_s.gold_mine_captures) as gold_mine_captures,
    sum(p_s.kraken_captures) as kraken_captures,
    sum(p_s.turret_captures) as turret_captures,
    sum(p_s.gold) as gold,
    sum(p_s.impact_score) as impact_score,
    sum(coalesce(p_i.surrender, 0)) as surrender,
    _p_i_item_uses_insert
from participant p
join participant_stats p_s on (p_s.participant_api_id = p.api_id)
left outer join participant_items p_i on (p_i.participant_api_id = p.api_id)
join filter f on (f.dimension_on = 'player' and (f.name = 'all' or f.id in (select gpf.filter_id from global_point_filters gpf where gpf.match_api_id = p.match_api_id)))
join series s on (p_s.created_at between s.start and s.end and s.dimension_on = 'player')
join hero h on (p.hero_id = h.id or h.name = 'all')
join role r on ((p.role_id = r.id and h.name = 'all') or r.name = 'all')  -- do not cross hero x role
-- join game_mode gm on ((p.game_mode_id = gm.id and h.name = 'all' and r.name = 'all') or gm.name = 'all')  -- do not cross mode x role / mode x hero
join game_mode gm on (p.game_mode_id = gm.id or gm.name = 'all')

where p.api_id in (:participant_api_ids)

group by p.player_api_id, s.id, f.id, h.id, gm.id, r.id
order by p.id

on duplicate key update
updated_at = case when values(updated_at) > updated_at then values(updated_at) else updated_at end,
played = played + values(played),
wins = wins + values(wins),
time_spent = time_spent + values(time_spent),
kills = kills + values(kills),
deaths = deaths + values(deaths),
assists = assists + values(assists),
farm = farm + values(farm),
minion_kills = minion_kills + values(minion_kills),
jungle_kills = jungle_kills + values(jungle_kills),
non_jungle_minion_kills = non_jungle_minion_kills + values(non_jungle_minion_kills),
crystal_mine_captures = crystal_mine_captures + values(crystal_mine_captures),
gold_mine_captures = gold_mine_captures + values(gold_mine_captures),
kraken_captures = kraken_captures + values(kraken_captures),
turret_captures = turret_captures + values(turret_captures),
gold = gold + values(gold),
impact_score = impact_score + values(impact_score),
surrender = surrender + values(surrender),
_p_i_item_uses_update
