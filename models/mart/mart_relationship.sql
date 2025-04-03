WITH relationship_time AS 
	(SELECT prep_votes.year
		, prep_votes.relationship 
		, prep_votes.to_country
		, prep_votes.from_country
		,(CASE WHEN from_start_year >= to_start_year THEN 
			from_start_year 
		ELSE 
			to_start_year END) AS relationship_start
		,(CASE WHEN from_last_year <= to_last_year THEN 
			from_last_year
		ELSE 
			to_last_year END) AS relationship_end
		FROM {{ref('prep_votes')}}
		WHERE round = 'final'),
	voting_system AS (SELECT relationship_time.relationship
		, relationship_time.to_country
		, relationship_time.from_country
		, relationship_time.year
		,(CASE WHEN ((relationship_time.YEAR >= relationship_start) AND (relationship_time.YEAR <= relationship_end) AND (relationship_time.YEAR>=2016)) THEN 1
			ELSE 0 END) AS new_voting_system
		,(CASE WHEN ((relationship_time.YEAR >= relationship_start) AND (relationship_time.YEAR <= relationship_end) AND (relationship_time.YEAR<2016)) THEN 1
			ELSE 0 END) AS old_voting_system
		FROM relationship_time),
	points AS (SELECT prep_votes.relationship
		, sum(total_points) AS points_earned
		FROM {{ref('prep_votes')}}
		WHERE round='final'
		GROUP BY prep_votes.relationship),
	old_points AS (SELECT prep_votes.relationship
		, SUM(total_points) AS old_points_earned
		FROM {{ref('prep_votes')}}
		WHERE (round='final' AND year < 2016)
		GROUP BY prep_votes.relationship),
	new_points AS (SELECT prep_votes.relationship
		, SUM(total_points) AS new_points_earned
		FROM {{ref('prep_votes')}}
		WHERE (round='final' AND year >= 2016)
		GROUP BY prep_votes.relationship),
	expected AS (SELECT voting_system.year
		, voting_system.relationship
		, voting_system.from_country
		, voting_system.to_country
		, SUM(new_voting_system)*24 AS new_points
		, SUM(old_voting_system)*12 AS old_points
		FROM voting_system
		GROUP BY voting_system.year
		, voting_system.relationship
		, voting_system.from_country
		, voting_system.to_country),
	ratio AS (SELECT points.points_earned
		, expected.year
		, expected.relationship
		, expect
		ed.from_country
		, expected.to_country
		, expected.new_points
		, expected.old_points
		, (expected.new_points+expected.old_points) AS total_possible
		FROM points
		JOIN expected ON expected.relationship=points.relationship)
SELECT ratio.YEAR
	, ratio.relationship
	, ratio.to_country
	, ratio.from_country
	, (CASE WHEN expected.new_points > 0 THEN new_points.new_points_earned/expected.new_points
	ELSE 0 END) AS new_point_ratio
	, (CASE WHEN expected.old_points > 0 THEN old_points.old_points_earned/expected.old_points
	ELSE 0 END) AS old_point_ratio
	, (CASE WHEN total_possible > 0 THEN points_earned/total_possible 
	ELSE 0 END) AS point_ratio
FROM ratio
JOIN new_points ON (new_points.relationship = ratio.relationship AND new_points.year = ratio.year)
JOIN old_points ON (old_points.relationship = ratio.relationship AND old_points.year = ratio.year)