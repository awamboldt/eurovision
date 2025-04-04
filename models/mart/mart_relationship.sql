WITH points AS (SELECT prep_votes.relationship
		, prep_votes.from_country
		, prep_votes.to_country
		, prep_votes.YEAR AS contest_year
		, prep_votes.total_points
		, (CASE WHEN prep_votes.YEAR >=2016 THEN prep_votes.total_points/24.0
		ELSE prep_votes.total_points/12.0 END) AS point_ratio
		, (CASE WHEN point_ratio = 1 THEN 1 ELSE 0 END) AS first_place
		FROM {{ref('prep_votes')}}
		WHERE round='final'),
	average AS (SELECT relationship
		, AVG(point_ratio) AS avg_point_ratio
		FROM points
		GROUP BY relationship)
SELECT points.*
, average.avg_point_ratio
, average.total_firsts
FROM points
JOIN average ON points.relationship = average.relationship