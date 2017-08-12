insert low_priority into `global_point_phase`
select
    null,
    now(),
    s.id as series_id,
    f.id as filter_id,
    h.id as hero_id,
    gm.id as game_mode_id,
    st.id as skill_tier_id,
    1 as build_id,  -- TODO
    r.id as role_id,
    reg.id as region_id,

    ph.start as phase_start,
    ph.end as phase_end,

    -- everything added here needs to be in on DUPLICATE too!!!
    count(p.id) as played,
    sum(p.winner),
    0 as time_spent,
    sum(kills),
    sum(deaths),
    sum(assists),
    sum(farm),
    sum(minion_kills),
    sum(jungle_kills),
    sum(non_jungle_minion_kills),
    sum(crystal_mine_captures),
    sum(gold_mine_captures),
    sum(kraken_captures),
    sum(turret_captures),
    sum(gold),
    sum(dmg_true_hero),
    sum(dmg_true_kraken),
    sum(dmg_true_turret),
    sum(dmg_true_vain_turret),
    sum(dmg_true_others),
    sum(dmg_dealt_hero),
    sum(dmg_dealt_kraken),
    sum(dmg_dealt_turret),
    sum(dmg_dealt_vain_turret),
    sum(dmg_dealt_others),
    sum(dmg_rcvd_dealt_hero),
    sum(dmg_rcvd_true_hero),
    sum(dmg_rcvd_dealt_others),
    sum(dmg_rcvd_true_others),
    sum(ability_a_level),
    sum(ability_b_level),
    sum(ability_c_level),
    sum(hero_level),

    0 as kda_ratio,
    0 as kill_participation,
    0 as cs_per_min,
    0 as kills_per_min
from participant_phases ph
join participant p on ph.participant_api_id = p.api_id
join filter f on (f.dimension_on = 'global' and (f.name = 'all' or f.id in (select gpf.filter_id from global_point_filters gpf where gpf.match_api_id = p.match_api_id)))
join series s on (p.created_at between s.start and s.end and s.dimension_on = 'global')
join hero h on (p.hero_id = h.id or h.name = 'all')
join role r on (p.role_id = r.id or r.name = 'all')
join region reg on (p.shard_id = reg.name or reg.name = 'all')
join game_mode gm on ((p.game_mode_id = gm.id and s.show_in_web = true) or gm.name = 'all')
join skill_tier st on ((p.skill_tier between st.start and st.end and s.show_in_web = true) or st.name = 'all')  -- no daily

where ph.id in (:participant_api_ids)  -- TODO rename

group by s.id, f.id, h.id, gm.id, st.id, r.id, reg.id, ph.start, ph.end
order by ph.id

on duplicate key update
played = played + values(played),
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
dmg_true_hero = dmg_true_hero + values(dmg_true_hero),
dmg_true_kraken = dmg_true_kraken + values(dmg_true_kraken),
dmg_true_turret = dmg_true_turret + values(dmg_true_turret),
dmg_true_vain_turret = dmg_true_vain_turret + values(dmg_true_vain_turret),
dmg_true_others = dmg_true_others + values(dmg_true_others),
dmg_dealt_hero = dmg_dealt_hero + values(dmg_dealt_hero),
dmg_dealt_kraken = dmg_dealt_kraken + values(dmg_dealt_kraken),
dmg_dealt_turret = dmg_dealt_turret + values(dmg_dealt_turret),
dmg_dealt_vain_turret = dmg_dealt_vain_turret + values(dmg_dealt_vain_turret),
dmg_dealt_others = dmg_dealt_others + values(dmg_dealt_others),
dmg_rcvd_dealt_hero = dmg_rcvd_dealt_hero + values(dmg_rcvd_dealt_hero),
dmg_rcvd_true_hero = dmg_rcvd_true_hero + values(dmg_rcvd_true_hero),
dmg_rcvd_dealt_others = dmg_rcvd_dealt_others + values(dmg_rcvd_dealt_others),
dmg_rcvd_true_others = dmg_rcvd_true_others + values(dmg_rcvd_true_others),
ability_a_level = ability_a_level + values(ability_a_level),
ability_b_level = ability_b_level + values(ability_b_level),
ability_c_level = ability_c_level + values(ability_c_level),
hero_level = hero_level + values(hero_level)
