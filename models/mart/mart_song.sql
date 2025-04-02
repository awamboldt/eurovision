WITH relationship_ratio AS 
    (SELECT to_country,
    AVG(point_ratio) AS point_ratio
    FROM {{ref('mart_relationship')}}
    GROUP BY to_country)
SELECT ps.*
	, rr.point_ratio
	, br.betting_odds
FROM {{ref('prep_songs')}} AS ps
JOIN relationship_ratio AS rr ON ps.country = rr.to_country