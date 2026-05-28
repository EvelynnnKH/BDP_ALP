"""Streamlit dashboard for Spark clickstream aggregation results.

Reads JSON snapshot files produced by spark_clickstream_aggregation_simple.py
and displays live event-type counts and batch history.
"""

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

DEFAULT_DASHBOARD_DIR = Path(os.getenv("DASHBOARD_DIR", "/app/dashboard_data"))

st.set_page_config(
    page_title="Clickstream Aggregation Dashboard",
    layout="wide",
)


def load_snapshot(dashboard_dir: Path) -> dict | None:
    snapshot_path = dashboard_dir / "latest_snapshot.json"
    if not snapshot_path.exists():
        return None
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def load_history(dashboard_dir: Path) -> list[dict]:
    history_path = dashboard_dir / "history.jsonl"
    if not history_path.exists():
        return []
    records = []
    for line in history_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records


# --- Sidebar (static — never reruns with the fragment) ---
with st.sidebar:
    st.title("Settings")
    dashboard_dir = Path(
        st.text_input("Dashboard data directory", str(DEFAULT_DASHBOARD_DIR))
    )
    refresh_seconds = st.slider("Auto-refresh interval (s)", 2, 30, 5)

# --- Header (static) ---
st.title("Clickstream Aggregation Dashboard")
st.caption(
    "Live event-type counts produced by Spark Structured Streaming "
    "(`spark_clickstream_aggregation_simple.py`)"
)


@st.fragment(run_every=refresh_seconds)
def live_dashboard():
    snapshot = load_snapshot(dashboard_dir)
    history = load_history(dashboard_dir)

    if snapshot is None:
        st.warning("No snapshot found. Start the Spark job first:")
        st.code(
            "docker exec -it week10-spark-master /opt/spark/bin/spark-submit \\\n"
            "  --master spark://spark-master:7077 \\\n"
            "  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 \\\n"
            "  /opt/week12/jobs/spark_clickstream_aggregation_simple.py",
            language="bash",
        )
        return

    rows = snapshot.get("rows", [])
    counts_df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["main_category", "count"])
    if not counts_df.empty:
        counts_df["count"] = counts_df["count"].astype(int)
        counts_df = counts_df.sort_values("count", ascending=False)

    total_events = int(counts_df["count"].sum()) if not counts_df.empty else 0

    # --- Metrics row ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Batch ID", snapshot.get("batch_id", "-"))
    col2.metric("Updated at", snapshot.get("updated_at", "-"))
    col3.metric("Total events (cumulative)", f"{total_events:,}")

    st.divider()

    # --- Charts and table ---
    left, right = st.columns([1, 1])

    with left:
        st.subheader("Events by type")
        if counts_df.empty:
            st.info("Waiting for first batch...")
        else:
            st.bar_chart(counts_df.set_index("main_category")["count"])

        if history:
            st.subheader("Total events per batch")
            history_df = pd.DataFrame(history)[["batch_id", "total_events"]]
            st.line_chart(history_df.set_index("batch_id"))

    with right:
        st.subheader("Current snapshot rows")
        if counts_df.empty:
            st.info("No rows yet.")
        else:
            st.dataframe(counts_df, use_container_width=True)

        if history:
            st.subheader("Batch history")
            history_df = pd.DataFrame(history)
            history_df = history_df[["batch_id", "updated_at", "total_events", "event_type_totals"]]
            history_df["event_type_totals"] = history_df["event_type_totals"].apply(
                lambda d: ", ".join(f"{k}: {v}" for k, v in sorted(d.items())) if isinstance(d, dict) else str(d)
            )
            history_df = history_df.sort_values("batch_id", ascending=False)
            st.dataframe(history_df, use_container_width=True)

    # --- How to read ---
    with st.expander("How to read this dashboard"):
        st.write(
            "Each Spark micro-batch runs every 10 seconds. "
            "The job reads all events from the Kafka topic since the earliest offset, "
            "groups them by `event_type`, and counts them. "
            "Because the output mode is `complete`, every batch shows the full running total — "
            "counts will keep growing as the producer sends new events. "
            "The batch history chart shows how quickly new events are arriving."
        )


live_dashboard()