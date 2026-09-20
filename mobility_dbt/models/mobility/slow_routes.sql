SELECT
    route_id,
    total_events,
    avg_speed_kmh,
    min_speed_kmh,
    max_speed_kmh,
    speed_category
FROM {{ ref('route_performance_summary') }}
WHERE speed_category = 'slow'
ORDER BY avg_speed_kmh ASC
