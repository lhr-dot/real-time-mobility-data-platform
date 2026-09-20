SELECT
    route_id,
    SUM(event_count) AS total_events,
    ROUND(AVG(avg_speed_kmh)::numeric, 2) AS avg_speed_kmh,
    MIN(avg_speed_kmh) AS min_speed_kmh,
    MAX(avg_speed_kmh) AS max_speed_kmh,
    CASE
        WHEN AVG(avg_speed_kmh) < 10 THEN 'slow'
        WHEN AVG(avg_speed_kmh) < 25 THEN 'normal'
        ELSE 'fast'
    END AS speed_category
FROM {{ ref('route_performance') }}
GROUP BY route_id
ORDER BY avg_speed_kmh ASC
