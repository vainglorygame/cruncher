insert low_priority into `global_point_hero_vs_hero`
select
    null,
    now(),
    s.id as series_id,
    f.id as filter_id,

    h.id as hero_id,
    r.id as role_id,
    h2.id as hero2_id,
    r2.id as role2_id,
    p.roster_api_id = p2.roster_api_id as played_with,

    gm.id as game_mode_id,
    st.id as skill_tier_id,
    reg.id as region_id,

    -- everything added here needs to be in on DUPLICATE too!!!
    count(p.id) as played,
    sum(cast(p.winner as INT)) as wins,
    sum(p.trueskill_delta) as trueskill_delta,
    sum(p_s.duration) as duration,
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
join participant p2 on p.match_api_id = p2.match_api_id and p.api_id <> p2.api_id
join filter f on (f.dimension_on = 'global' and (f.name = 'all' or f.id in (select gpf.filter_id from global_point_filters gpf where gpf.match_api_id = p.match_api_id)))
join series s on (p_s.created_at between s.start and s.end and s.dimension_on = 'global' and s.show_in_web = true)  -- no daily
join hero h on p.hero_id = h.id
join role r on p.role_id = r.id
join hero h2 on p2.hero_id = h2.id
join role r2 on p2.role_id = r2.id
join region reg on (p.shard_id = reg.name or reg.name = 'all')
join game_mode gm on ((p.game_mode_id = gm.id and s.show_in_web = true) or gm.name = 'all')
join skill_tier st on (p.skill_tier between st.start and st.end or st.name = 'all')

where p.api_id in (:participant_api_ids)

group by s.id, f.id, h.id, r.id, h2.id, r2.id, played_with, gm.id, st.id, reg.id
order by p.id

on duplicate key update
played = played + values(played),
wins = wins + values(wins),
trueskill_delta = trueskill_delta + values(trueskill_delta),
duration = duration + values(duration),
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
