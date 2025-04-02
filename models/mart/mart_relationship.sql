WITH relationship_time AS
	(SELECT relationship 
		, to_country
		,(CASE WHEN from_start_year >= to_start_year THEN 
			from_start_year 
		ELSE 
			to_start_year END) AS relationship_start
		,(CASE WHEN from_last_year <= to_last_year THEN 
			from_last_year
		ELSE 
			to_last_year END) AS relationship_end
	FROM {{ref('prep_votes')}},
voting_system AS 
	(SELECT relationship_time.relationship
		, relationship_time.to_country
		,(CASE WHEN relationship_start >= 2016 THEN relationship_end-relationship_start
			ELSE relationship_end-2016 END) AS new_voting_system
		,(CASE WHEN relationship_start >= 2016 THEN relationship_start*0
			ELSE 2016-relationship_start END) AS old_voting_system
	FROM relationship_time),
points AS 
	(SELECT prep_votes.relationship
		, sum(total_points) AS points_earned
	FROM {{ref('prep_votes')}}
	GROUP BY prep_votes.relationship),
expected AS 
	(SELECT voting_system.relationship
		, voting_system.to_country
		, new_voting_system*24 AS new_points
		, old_voting_system*12 AS old_points
	FROM voting_system),
ratio AS 
	(SELECT points.points_earned
		, expected.relationship
		, expected.to_country
		, (expected.new_points+expected.old_points) AS total_possible
	FROM points
	JOIN expected ON expected.relationship=points.relationship)
SELECT ratio.relationship,
	ratio.to_country,
	points_earned/total_possible AS point_ratio
FROM ratio