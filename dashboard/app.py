"""
Streamlit dashboard for Spark clickstream aggregation results
"""

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

DEFAULT_DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR", "/app/dashboard_data"))

st.set_page_config(
    page_title="E-Commerce Clickstream Dashboard",
    layout="wide",
    page_icon="🛍️"
)

# ===========
# CUSTOM CSS 
# ===========
st.markdown("""
    <style>
    /* Header banner */
    .header-container {
        background-color: #1C1C1C;
        padding: 24px;
        border-radius: 12px;
        border-left: 6px solid #E23744;
        margin-bottom: 25px;
    }
    .header-title {
        color: #FFFFFF !important;
        margin: 0;
        font-size: 32px;
        font-weight: 800;
    }
    .header-sub {
        color: #CCCCCC !important;
        margin: 8px 0 0 0;
        font-size: 14px;
    }
    .header-badge {
        color: #FF6B76 !important;
        margin: 6px 0 0 0;
        font-size: 12px;
        font-weight: 700;
    }

    /* Metric cards */
    .metric-card {
        background-color: #1C1C1C;
        border: 1px solid #333333;
        padding: 20px;
        border-radius: 12px;
        text-align: left;
    }
    .metric-label {
        color: #AAAAAA !important;
        font-size: 12px;
        margin-bottom: 6px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.6px;
    }
    .metric-value {
        color: #FFFFFF !important;
        font-size: 26px;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-value-red {
        color: #FF6B76 !important;
        font-size: 26px;
        font-weight: 700;
        line-height: 1.2;
    }
    .metric-value-sm {
        color: #FFFFFF !important;
        font-size: 15px;
        font-weight: 600;
        padding-top: 6px;
        line-height: 1.4;
    }

    /* Insight cards */
    .insight-card {
        background-color: #1C1C1C;
        border: 1px solid #333333;
        border-top: 4px solid #E23744;
        padding: 18px;
        border-radius: 8px;
        min-height: 120px;
    }
    .insight-title {
        color: #FFFFFF !important;
        font-size: 15px;
        font-weight: 700;
        margin-bottom: 8px;
    }
    .insight-desc {
        color: #CCCCCC !important;
        font-size: 13px;
        line-height: 1.7;
    }
    .insight-desc b {
        color: #FF6B76 !important;
    }
    </style>
""", unsafe_allow_html=True)


# =========================
# DATA LOADER
# =========================
def load_snapshot(dashboard_dir: Path) -> dict | None:
    snapshot_path = dashboard_dir / "latest_snapshot.json"
    if not snapshot_path.exists():
        return None
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def load_history(dashboard_dir: Path) -> list[dict]:
    history_path = dashboard_dir / "history.jsonl"
    if not history_path.exists():
        return []
    return [json.loads(l) for l in history_path.read_text().splitlines() if l.strip()]


# =========================
# SIDEBAR
# =========================
with st.sidebar:
    st.title("🛒 Clickstream App")

    page = st.selectbox("Pilih Halaman:", ["🏠 Home / Overview", "📈 Deep Analytics"])

    dashboard_dir = DEFAULT_DASHBOARD_DIR

    refresh_seconds = st.slider("Auto Refresh (sec)", 2, 30, 5)

    st.markdown("---")
    st.caption("Spark → Kafka → Streamlit Dashboard")


# =========================
# MAIN DASHBOARD
# =========================
@st.fragment(run_every=refresh_seconds)
def live_dashboard():

    snapshot = load_snapshot(dashboard_dir)
    history = load_history(dashboard_dir)

    if snapshot is None:
        st.warning("⚠ No snapshot found. Start Spark job first.")
        st.code("""
docker exec -it week10-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \
  /opt/week12/jobs/spark_clickstream_aggregation_simple.py
        """)
        return

    rows = snapshot.get("rows", [])
    counts_df = pd.DataFrame(rows)

    if not counts_df.empty:
        counts_df["count"] = counts_df["count"].astype(int)
        counts_df = counts_df.sort_values("count", ascending=False)

    total_events = int(counts_df["count"].sum()) if not counts_df.empty else 0
    top_category = counts_df.iloc[0]["main_category_name"] if not counts_df.empty else "-"
    batch_id = snapshot.get("batch_id", "-")
    updated_at = snapshot.get("updated_at", "-")

    # ── HEADER ──
    st.markdown(f"""
        <div class="header-container">
            <p class="header-title">🛍️ Clickstream Dashboard</p>
            <p class="header-sub">Real-Time Customer Behavior & Online Shopping Operational Insights</p>
            <p class="header-badge">Batch ID: {batch_id} &nbsp;|&nbsp; Streaming status: Active</p>
        </div>
    """, unsafe_allow_html=True)

    # ── METRIC CARDS ──
    m1, m2, m3, m4 = st.columns(4)

    with m1:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">🔥 Total Events</div>
                <div class="metric-value">{total_events:,}</div>
            </div>
        """, unsafe_allow_html=True)

    with m2:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">📦 Batch ID</div>
                <div class="metric-value">{batch_id}</div>
            </div>
        """, unsafe_allow_html=True)

    with m3:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">🏷️ Top Category</div>
                <div class="metric-value-red">{top_category}</div>
            </div>
        """, unsafe_allow_html=True)

    with m4:
        st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">🕒 Last Update</div>
                <div class="metric-value-sm">{updated_at}</div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ==========================================
    # HALAMAN 1: HOME / OVERVIEW
    # ==========================================
    if page == "🏠 Home / Overview":

        left, right = st.columns([1.3, 1])

        with left:
            st.markdown("### 📊 Category Distribution (Current Batch)")
            if counts_df.empty:
                st.info("Waiting for data...")
            else:
                st.bar_chart(counts_df.set_index("main_category_name")["count"], color="#E23744")

            if history:
                st.markdown("### 📈 Event Trend per Batch")
                history_df = pd.DataFrame(history)[["batch_id", "total_events"]]
                history_df["batch_id"] = pd.to_numeric(history_df["batch_id"])
                history_df = history_df.sort_values("batch_id")
                st.line_chart(history_df.set_index("batch_id"), color="#E23744")

                st.markdown("### 📈 Cumulative Total Processed Events")
                history_df["cumulative_events"] = history_df["total_events"].cumsum()
                st.line_chart(history_df.set_index("batch_id")["cumulative_events"], color="#E23744")

        with right:
            st.markdown("### 🎨 Trending Colors")
            color_rows = snapshot.get("color_rows", [])
            color_df = pd.DataFrame(color_rows, columns=["colour_name", "count"])

            if color_df.empty:
                st.info("No color data available in snapshot. Group by 'colour' in Spark to activate.")
            else:
                color_df["count"] = color_df["count"].astype(int)
                color_df = color_df.sort_values("count", ascending=False)

                st.dataframe(
                    color_df,
                    column_config={
                        "colour_name": "Product Color",
                        "count": st.column_config.ProgressColumn(
                            "Total Clicks",
                            format="%d",
                            min_value=0,
                            max_value=int(color_df["count"].max())
                        )
                    },
                    hide_index=True,
                    use_container_width=True
                )

                st.markdown("### 📊 Visual Color Preferences")
                st.bar_chart(color_df.set_index("colour_name")["count"], horizontal=True, color="#E23744")

            st.markdown("### 📋 Live Data Feed Table")
            if counts_df.empty:
                st.info("No data yet")
            else:
                st.dataframe(counts_df, use_container_width=True, hide_index=True)

        st.markdown("---")

        # ── QUICK INSIGHTS ──
        st.markdown("## 💡 Quick Insights")
        i1, i2, i3 = st.columns(3)

        with i1:
            st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">🚀 Peak Session Demand</div>
                    <div class="insight-desc">
                        Kategori pakaian <b>{top_category}</b> saat ini memimpin dominasi trafik
                        pencarian dengan mencatatkan akumulasi klik paling tinggi dibanding produk
                        retail lainnya.
                    </div>
                </div>
            """, unsafe_allow_html=True)

        with i2:
            st.markdown("""
                <div class="insight-card">
                    <div class="insight-title">🎨 Color Preferences</div>
                    <div class="insight-desc">
                        Visualisasi progress bar di atas menunjukkan preferensi warna konsumen
                        secara real-time. Melacak warna tren membantu optimalisasi stok
                        pergudangan e-shop.
                    </div>
                </div>
            """, unsafe_allow_html=True)

        with i3:
            st.markdown(f"""
                <div class="insight-card">
                    <div class="insight-title">📦 Stream Performance</div>
                    <div class="insight-desc">
                        Berhasil memproses total <b>{total_events:,}</b> log aktivitas belanja
                        dalam batch ke-<b>{batch_id}</b> langsung dari klaster Apache Spark
                        Data Pipeline.
                    </div>
                </div>
            """, unsafe_allow_html=True)

    # ==========================================
    # HALAMAN 2: DEEP ANALYTICS
    # ==========================================
    elif page == "📈 Deep Analytics":

        st.markdown("## 📊 Deep Analytics View")

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            st.markdown("### 🏆 Top 10 Most Viewed Products")
            product_rows = snapshot.get("product_rows", [])
            product_df = pd.DataFrame(product_rows, columns=["product_model", "clicks"])

            if product_df.empty:
                st.info("No product data available. Group by 'clothing_model' in Spark to activate.")
            else:
                product_df["clicks"] = product_df["clicks"].astype(int)
                product_df = product_df.sort_values("clicks", ascending=False).head(10)
                st.bar_chart(product_df.set_index("product_model")["clicks"], horizontal=True, color="#E23744")

        with chart_col2:
            st.markdown("### 💰 Price Sensitivity Analysis")
            price_rows = snapshot.get("price_analytics", [])
            price_df = pd.DataFrame(price_rows, columns=["main_category_name", "avg_price", "total_clicks"])

            if price_df.empty:
                st.info("No price analytics available. Aggregate 'price' and 'count' in Spark to activate.")
            else:
                price_df["avg_price"] = price_df["avg_price"].astype(float)
                price_df["total_clicks"] = price_df["total_clicks"].astype(int)
                st.scatter_chart(
                    data=price_df,
                    x="avg_price",
                    y="total_clicks",
                    color="main_category_name",
                    size="total_clicks"
                )

        st.markdown("---")

        if history:
            history_df = pd.DataFrame(history)
            st.markdown("### Batch History Table")

            col = "event_type_totals" if "event_type_totals" in history_df.columns else "category_totals"

            if col in history_df.columns:
                history_df[col] = history_df[col].apply(
                    lambda d: ", ".join(f"{k}:{v}" for k, v in d.items()) if isinstance(d, dict) else str(d)
                )
                display_cols = ["batch_id", "updated_at", "total_events", col]
            else:
                display_cols = ["batch_id", "updated_at", "total_events"]

            st.dataframe(
                history_df[display_cols],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No history data available")

    # ── FOOTER ──
    with st.expander("ℹ How it works"):
        st.write("""
        - **Spark Streaming:** Membaca data log clickstream pakaian e-shop secara real-time lewat Kafka.
        - **Data Aggregations:** Mengelompokkan data berdasarkan kategori utama, warna produk, model produk, dan rata-rata harga produk.
        - **Dashboard Refresh:** State dashboard diperbarui secara otomatis menggunakan `@st.fragment` berdasarkan interval waktu yang dipilih di sidebar.
        """)


live_dashboard()
