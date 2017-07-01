insert low_priority into :crunch_table
select
    null,
    now(),
    s.id as series_id,
    f.id as filter_id,
    h.id as hero_id,
    gm.id as game_mode_id,
    st.id as skill_tier_id,
    b.id as build_id,
    r.id as role_id,
    1,
    1,
    reg.id as region_id,

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
    sum(p_s.impact_score) as impact_score
from participant p
join participant_stats p_s on (p_s.participant_api_id = p.api_id)
join filter f on ((f.name = 'all' and f.dimension_on = 'global') or f.id in (select gpf.filter_id from global_point_filters gpf where gpf.match_api_id = p.match_api_id))
join series s on (p_s.created_at between s.start and s.end and s.dimension_on = 'global')
join hero h on (p.hero_id = h.id or h.name = 'all')
join role r on (p.role_id = r.id or r.name = 'all')
join region reg on (p.shard_id = reg.name or reg.name = 'all')
join game_mode gm on ((p.game_mode_id = gm.id and s.show_in_web = true) or gm.name = 'all')
join skill_tier st on ((p.skill_tier between st.start and st.end and s.show_in_web = true) or st.name = 'all')  -- no daily
join build b on ((  -- only per patch global
    s.show_in_web = true and
    gm.name = 'all' and
    st.name = 'all' and
    reg.name = 'all' and

    (b.item_1 is null or p_s.item_grants RLIKE concat(:build_regex_start, b.item_1, ';', b.item_1_count, :build_regex_end)) and
    (b.item_2 is null or p_s.item_grants RLIKE concat(:build_regex_start, b.item_2, ';', b.item_2_count, :build_regex_end)) and
    (b.item_3 is null or p_s.item_grants RLIKE concat(:build_regex_start, b.item_3, ';', b.item_3_count, :build_regex_end)) and
    (b.item_4 is null or p_s.item_grants RLIKE concat(:build_regex_start, b.item_4, ';', b.item_4_count, :build_regex_end)) and
    (b.item_5 is null or p_s.item_grants RLIKE concat(:build_regex_start, b.item_5, ';', b.item_5_count, :build_regex_end)) and
    (b.item_6 is null or p_s.item_grants RLIKE concat(:build_regex_start, b.item_6, ';', b.item_6_count, :build_regex_end)) and
    b.dimension_on = 'global'
) or b.name = 'all')

where p.api_id in (:participant_api_ids)

group by s.id, f.id, h.id, gm.id, st.id, b.id, r.id, reg.id -- h2.id, r2.id, reg.id
order by p.id

on duplicate key update
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
impact_score = impact_score + values(impact_score)
