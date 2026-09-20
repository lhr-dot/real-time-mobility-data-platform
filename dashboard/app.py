import pandas as pd
import streamlit as st
import psycopg2

st.set_page_config(
    page_title="TBM Mobility Dashboard",
    page_icon="🚌",
    layout="wide",
)

st.title("🚌 TBM Real-Time Mobility Dashboard")
st.caption("Analyse des performances des lignes à partir des données temps réel.")

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="mobility",
    user="mobility",
    password="mobility",
)

summary = pd.read_sql(
    """
    SELECT
        route_id,
        total_events,
        avg_speed_kmh,
        min_speed_kmh,
        max_speed_kmh,
        speed_category
    FROM analytics.route_performance_summary
    ORDER BY avg_speed_kmh ASC
    """,
    conn,
)

history = pd.read_sql(
    """
    SELECT
        route_id,
        event_count,
        avg_speed_kmh,
        processed_at
    FROM analytics.route_speed_history
    ORDER BY processed_at ASC
    """,
    conn,
)

conn.close()

st.sidebar.header("Filtres")

categories = ["Toutes"] + sorted(
    summary["speed_category"].dropna().unique().tolist()
)

selected_category = st.sidebar.selectbox(
    "Catégorie de vitesse",
    categories,
)

filtered = summary.copy()

if selected_category != "Toutes":
    filtered = filtered[
        filtered["speed_category"] == selected_category
    ]

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Lignes observées",
    filtered["route_id"].nunique(),
)

col2.metric(
    "Vitesse moyenne",
    f"{filtered['avg_speed_kmh'].mean():.1f} km/h",
)

col3.metric(
    "Lignes lentes",
    int((filtered["speed_category"] == "slow").sum()),
)

col4.metric(
    "Événements",
    int(filtered["total_events"].sum()),
)

st.divider()

st.subheader("📈 Évolution de la vitesse")

if not history.empty:
    selected_route = st.selectbox(
        "Choisir une ligne",
        sorted(history["route_id"].dropna().unique().tolist()),
    )

    route_history = history[
        history["route_id"] == selected_route
    ].copy()

    route_history["processed_at"] = pd.to_datetime(
        route_history["processed_at"]
    )

    chart_data = (
        route_history[
            ["processed_at", "avg_speed_kmh"]
        ]
        .set_index("processed_at")
        .sort_index()
    )

    st.line_chart(chart_data)

    st.caption(
        f"Évolution de la vitesse moyenne de la ligne {selected_route}"
    )
else:
    st.info("Pas encore suffisamment de données historiques.")

st.divider()

st.subheader("🚍 Vitesse moyenne par ligne")

chart_data = (
    filtered[
        ["route_id", "avg_speed_kmh"]
    ]
    .set_index("route_id")
    .sort_values("avg_speed_kmh")
)

st.bar_chart(chart_data)

st.subheader("📊 Répartition des catégories")

category_counts = (
    filtered["speed_category"]
    .value_counts()
    .rename_axis("category")
    .reset_index(name="count")
)

st.bar_chart(
    category_counts.set_index("category")
)

st.subheader("🐌 Lignes lentes")

slow_routes = filtered[
    filtered["speed_category"] == "slow"
].copy()

st.dataframe(
    slow_routes,
    use_container_width=True,
    hide_index=True,
)

st.subheader("Performance des lignes")

st.dataframe(
    filtered,
    use_container_width=True,
    hide_index=True,
)
