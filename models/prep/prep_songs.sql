select s.*
, c.normal_cluster
from {{source('extracted_data', 'songs_info_with_2013')}} AS s
JOIN {{source('extracted_data', 'clusters')}} AS c ON (c.year = s.year AND s.artist_name = c.artist_name)