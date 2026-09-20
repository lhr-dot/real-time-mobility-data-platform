SELECT
    route_id,
    event_count,
    ROUND(avg_speed_kmh::numeric, 2) AS avg_speed_kmh,
    CASE
        WHEN avg_speed_kmh < 10 THEN 'slow'
        WHEN avg_speed_kmh < 25 THEN 'normal'
        ELSE 'fast'
    END AS speed_category,
    processed_at
FROM {{ source('mobility', 'route_speed_metrics') }}
