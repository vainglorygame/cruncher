UPDATE team_membership tm
JOIN (
    SELECT
    tm.id AS tm_id,
    SUM(
        (CASE
            WHEN tm_cnt=1 THEN 0.33
            WHEN tm_cnt=2 THEN 0.66
            WHEN tm_cnt=3 THEN 1.33
        END) *
        (CASE
            WHEN p.winner=TRUE THEN 1
            WHEN p.winner=FALSE THEN 0.7
        END) *
        (CASE
            WHEN tm.status='initiate' THEN 10
            WHEN tm.status='member' THEN 75
            WHEN tm.status='veteran' THEN 100
            WHEN tm.status='officer' THEN 125
            WHEN tm.status='leader' THEN 125
        END)
    ) AS fame
    FROM (
        SELECT
        t.id AS t_id,
        m.api_id AS m_api_id,
        COUNT(tm.id) AS tm_cnt
        FROM participant p
        JOIN player pl ON p.player_api_id = pl.api_id
        JOIN team_membership tm ON pl.api_id = tm.player_api_id
        JOIN team t ON tm.team_id = t.id
        JOIN roster r ON p.roster_api_id = r.api_id
        JOIN `match` m ON r.match_api_id = m.api_id
        WHERE t.id = :team_id
        AND p.created_at > DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        GROUP BY t.id, m.api_id, r.id
    ) AS cnt_by_m
    JOIN participant p ON p.match_api_id = cnt_by_m.m_api_id
    JOIN team_membership tm ON cnt_by_m.t_id = tm.team_id AND tm.player_api_id = p.player_api_id
    GROUP BY tm.id
) AS fame_diff ON tm.id = fame_diff.tm_id
SET tm.fame = fame_diff.fame
