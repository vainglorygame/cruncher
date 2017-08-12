insert low_priority into `global_point_bans`
select
    null,
    now(),
    s.id as series_id,
    f.id as filter_id,
    h.id as hero_id,
    gm.id as game_mode_id,
    st.id as skill_tier_id,
    reg.id as region_id,

    -- everything added here needs to be in on DUPLICATE too!!!
    count(p.id) as played,
    0 as banned
from participant_phases ph
join participant p on ph.participant_api_id = p.api_id
join filter f on (f.dimension_on = 'global' and (f.name = 'all' or f.id in (select gpf.filter_id from global_point_filters gpf where gpf.match_api_id = p.match_api_id)))
join series s on (p.created_at between s.start and s.end and s.dimension_on = 'global')
join hero h on (ph.ban = h.id or h.name = 'all')  -- special to ban table!
join region reg on (p.shard_id = reg.name or reg.name = 'all')
join game_mode gm on ((p.game_mode_id = gm.id and s.show_in_web = true) or gm.name = 'all')
join skill_tier st on ((p.skill_tier between st.start and st.end and s.show_in_web = true) or st.name = 'all')  -- no daily

where ph.id in (:participant_api_ids)  -- TODO rename

group by s.id, f.id, h.id, gm.id, st.id, reg.id
order by ph.id

on duplicate key update
played = played + values(played)
