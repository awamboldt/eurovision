SELECT B.*
, m.final_place
FROM {{source('extracted_data', 'betting')}} AS b
JOIN {{ref('mart_song')}} AS m ON m.YEAR = b.YEAR AND m.artist_name = b.performer