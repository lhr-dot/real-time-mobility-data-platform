SELECT
    route_id,
    event_count,
    ROUND(avg_speed_kmh::numeric, 2) AS avg_speed_kmh,
    processed_at
FROM {{ source('mobility', 'route_speed_history') }}
WHERE avg_speed_kmh IS NOT NULL
