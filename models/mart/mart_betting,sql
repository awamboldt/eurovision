SELECT YEAR
	, country_name
	, AVG(betting_score) AS betting_odds
FROM {{ref('prep_betting')}}
GROUP BY YEAR, country_name