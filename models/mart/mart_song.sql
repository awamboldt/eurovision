WITH relationship_ratio AS 
    (SELECT to_country,
    AVG(point_ratio) AS point_ratio
    FROM {{ref('mart_relationship')}}
    GROUP BY to_country)
, betting_ratio AS
	(SELECT YEAR
	, performer
	, song
	, AVG(betting_score) AS betting_odds
	FROM {{ref('prep_betting')}}
	GROUP BY YEAR, performer, song)
SELECT ps.*
	, rr.point_ratio
	, br.betting_odds
FROM {{ref('prep_songs')}} AS ps
JOIN relationship_ratio AS rr ON ps.country = rr.to_country
JOIN betting_ratio AS br ON ((ps.YEAR = br.YEAR) AND (ps.artist_name = br.performer) AND (ps.song_name = br.song))