WITH inverse AS (SELECT
	 mart_relationship.contest_year
	 , mart_relationship.to_country
	 , mart_relationship.from_country
	, mart_relationship.relationship AS original_relationship
	, mart_relationship.point_ratio AS original_ratio
	, CONCAT(to_country,'-',from_country) AS inverse_relationship
	, mart_relationship.avg_point_ratio AS original_avg_ratio
	FROM {{ref('mart_relationship')}}),
intermediate AS (SELECT 
	inverse.contest_year
	, inverse.original_relationship
	, inverse.inverse_relationship
	, inverse.original_ratio
	, m.point_ratio AS inverse_ratio
	, (inverse.original_ratio-m.point_ratio) AS year_reciprocity
	, inverse.original_avg_ratio
	FROM {{ref('mart_relationship')}} AS m
	JOIN inverse ON (m.relationship = inverse.inverse_relationship AND m.contest_year = inverse.contest_year)),
average AS (SELECT i.*
	, AVG(i.inverse_ratio) AS inverse_avg_ratio
	FROM intermediate AS i
	GROUP BY CONTEST_YEAR
	, i.original_relationship
	, i.original_ratio
	, i.INVERSE_RELATIONSHIP 
	, i.INVERSE_RATIO 
	, i.YEAR_RECIPROCITY 
	, i.original_avg_ratio)
SELECT *
,  original_avg_ratio-inverse_avg_ratio AS avg_reciprocity
FROM average