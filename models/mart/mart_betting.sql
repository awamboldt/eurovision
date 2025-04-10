WITH average as(SELECT
YEAR
, country_name
, AVG(betting_score) AS betting_odds
FROM {{ref('prep_betting')}}
WHERE contest_round = 'final' 
GROUP BY YEAR, country_name)
SELECT  p.YEAR
, p.country_name
, p.final_place
, a.betting_odds
, p.performer
, p.song
, p.betting_name
, p.betting_score
, p.page_url
, p.contest_round
, p.country_code
, p.betting_bm_id
, p.betting_sc_id
FROM {{ref('prep_betting')}} AS p
JOIN average AS A ON a.YEAR = p.YEAR AND a.country_name = p.COUNTRY_NAME
ORDER BY YEAR, country_name