import time
import json
from typing import Dict, Any, List, Tuple

import requests
import streamlit as st


# =========================
# "Kafka" — lightweight, in-memory simulation
# =========================
def _init_broker():
    if "topics" not in st.session_state:
        st.session_state.topics = {"api_status": []}  # topic -> append-only list (the "log")


def publish(topic: str, message: Dict[str, Any]) -> None:
    """Append a message to the topic log with a server timestamp."""
    message = dict(message)  # shallow copy
    message["topic"] = topic
    message["ts"] = time.time()
    st.session_state.topics[topic].append(message)


def read_last(topic: str, n: int = 50) -> List[Dict[str, Any]]:
    """Read the last n messages from a topic (like tail)."""
    return st.session_state.topics.get(topic, [])[-n:]


# =========================
# Agent 1 — API Monitor Agent
# =========================
def _timed_get(url: str, timeout: float = 6.0) -> Tuple[float, requests.Response | None, str | None]:
    """Return (latency_sec, response_or_none, error_or_none)."""
    t0 = time.perf_counter()
    try:
        r = requests.get(url, timeout=timeout)
        latency = time.perf_counter() - t0
        return latency, r, None
    except Exception as e:
        latency = time.perf_counter() - t0
        return latency, None, str(e)


def check_open_meteo(lat: float, lon: float) -> Dict[str, Any]:
    url = (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}&current_weather=true"
    )
    latency, resp, err = _timed_get(url)
    if err or resp is None:
        return {"api": "Open-Meteo", "status": "DOWN", "latency_s": latency, "error": err}

    ok = resp.status_code == 200
    data = {}
    try:
        data = resp.json().get("current_weather", {})
    except Exception:
        pass

    if ok and data:
        return {
            "api": "Open-Meteo",
            "status": "OK",
            "latency_s": latency,
            "temperature_c": data.get("temperature"),
            "windspeed": data.get("windspeed"),
            "weathercode": data.get("weathercode"),
        }
    return {"api": "Open-Meteo", "status": "DOWN", "latency_s": latency, "code": resp.status_code}


def check_coindesk_btc() -> Dict[str, Any]:
    # Public, free BTC price
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    latency, resp, err = _timed_get(url)
    if err or resp is None:
        return {"api": "CoinDesk BTC", "status": "DOWN", "latency_s": latency, "error": err}

    try:
        ok = resp.status_code == 200
        payload = resp.json()
        price = payload["bpi"]["USD"]["rate_float"] if ok else None
        if ok and price is not None:
            return {"api": "CoinDesk BTC", "status": "OK", "latency_s": latency, "price_usd": float(price)}
        return {"api": "CoinDesk BTC", "status": "DOWN", "latency_s": latency, "code": resp.status_code}
    except Exception as e:
        return {"api": "CoinDesk BTC", "status": "DOWN", "latency_s": latency, "error": str(e)}


def check_ipify() -> Dict[str, Any]:
    # Tiny utility API to diversify signals (also free/no key)
    url = "https://api.ipify.org?format=json"
    latency, resp, err = _timed_get(url)
    if err or resp is None:
        return {"api": "IPify", "status": "DOWN", "latency_s": latency, "error": err}
    try:
        ok = resp.status_code == 200
        ip = resp.json().get("ip") if ok else None
        if ok and ip:
            return {"api": "IPify", "status": "OK", "latency_s": latency, "ip": ip}
        return {"api": "IPify", "status": "DOWN", "latency_s": latency, "code": resp.status_code}
    except Exception as e:
        return {"api": "IPify", "status": "DOWN", "latency_s": latency, "error": str(e)}


def api_monitor_agent(lat: float, lon: float) -> List[Dict[str, Any]]:
    """Poll multiple free APIs and publish each status to the 'api_status' topic."""
    results = [
        check_open_meteo(lat, lon),
        check_coindesk_btc(),
        check_ipify(),
    ]
    for r in results:
        publish("api_status", r)
    return results


# =========================
# Agent 2 — Kafka Status Agent (consumer + analysis)
# =========================
def analyze_api_status(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Look for anomalies & summarize stream health."""
    summary = {
        "counts": {},
        "latest_by_api": {},
        "anomalies": [],
        "latency_stats": {},
    }

    # Aggregations
    latencies_by_api = {}
    last_price = None
    prev_price = None
    open_meteo_temps = []

    for m in messages:
        api = m.get("api", "unknown")
        status = m.get("status", "unknown")
        summary["counts"].setdefault(api, {"OK": 0, "DOWN": 0, "unknown": 0})
        summary["counts"][api][status] = summary["counts"][api].get(status, 0) + 1
        summary["latest_by_api"][api] = m

        # latency
        lat = m.get("latency_s")
        if lat is not None:
            latencies_by_api.setdefault(api, []).append(float(lat))

        # BTC price anomaly detection (simple % jump)
        if api == "CoinDesk BTC" and status == "OK" and "price_usd" in m:
            prev_price, last_price = last_price, m["price_usd"]
            if prev_price and last_price:
                change_pct = 100.0 * (last_price - prev_price) / prev_price
                if abs(change_pct) >= 5.0:  # alert on >= ±5%
                    summary["anomalies"].append(
                        {
                            "type": "price_jump",
                            "api": "CoinDesk BTC",
                            "prev_price": round(prev_price, 2),
                            "new_price": round(last_price, 2),
                            "change_pct": round(change_pct, 2),
                        }
                    )

        # Temperature sanity check
        if api == "Open-Meteo" and status == "OK" and "temperature_c" in m:
            open_meteo_temps.append(m["temperature_c"])

    # Multiple DOWNs heuristic
    for api, counts in summary["counts"].items():
        if counts.get("DOWN", 0) >= 3:
            summary["anomalies"].append({"type": "repeated_down", "api": api, "count": counts["DOWN"]})

    # Temperature plausibility
    if open_meteo_temps:
        tmin, tmax = min(open_meteo_temps), max(open_meteo_temps)
        if tmin < -60 or tmax > 60:
            summary["anomalies"].append(
                {"type": "temp_out_of_range", "api": "Open-Meteo", "min_c": tmin, "max_c": tmax}
            )

    # Latency stats
    for api, arr in latencies_by_api.items():
        summary["latency_stats"][api] = {
            "count": len(arr),
            "p50_s": round(sorted(arr)[len(arr) // 2], 3),
            "max_s": round(max(arr), 3),
            "avg_s": round(sum(arr) / len(arr), 3),
        }

    return summary


# =========================
# Streamlit UI (Agentic actions)
# =========================
st.set_page_config(page_title="Agentic AI + (Simulated) Kafka + Free APIs", page_icon="🛰️", layout="wide")
_init_broker()

st.title("🛰️ Agentic AI + Kafka (Simulated) + Free APIs")
st.caption(
    "Two agentic agents: (1) API Monitor Agent publishes health & data to a simulated Kafka topic; "
    "(2) Kafka Status Agent consumes & analyzes for anomalies. All free, all in one Streamlit app."
)

# Sidebar: config
st.sidebar.header("Configuration")
lat = st.sidebar.number_input("Latitude", value=28.6139, help="Default: New Delhi approx.")
lon = st.sidebar.number_input("Longitude", value=77.2090)
tail_n = st.sidebar.slider("Messages to analyze (tail)", min_value=10, max_value=200, value=50, step=10)
auto_poll = st.sidebar.toggle("Auto-poll every refresh", value=False)
show_actions = st.sidebar.toggle("Show Agentic Actions JSON", value=True)

# Agentic "actions" descriptor (machine-readable-ish)
actions_descriptor = {
    "actions": [
        {
            "name": "POLL_APIS",
            "description": "Poll Open-Meteo, CoinDesk BTC, IPify and publish status to topic 'api_status'.",
            "inputs": {"latitude": "float", "longitude": "float"},
            "outputs": {"topic": "api_status", "message_schema": "{api, status, latency_s, ...}"},
        },
        {
            "name": "ANALYZE_TOPIC",
            "description": "Consume last N messages from 'api_status' and detect anomalies.",
            "inputs": {"tail_n": "int"},
            "outputs": {"summary": "{counts, latest_by_api, anomalies, latency_stats}"},
        },
        {
            "name": "EXPORT_LOG",
            "description": "Download the entire topic log as JSON.",
            "inputs": {},
            "outputs": {"file": "api_status.json"},
        },
    ]
}

# Layout
left, right = st.columns([1, 1], gap="large")

with left:
    st.subheader("Agent 1 — API Monitor Agent")
    if st.button("POLL_APIS (Publish to Topic)"):
        results = api_monitor_agent(lat, lon)
        st.success("Published messages to topic 'api_status'.")
        st.json(results)

    if auto_poll:
        results = api_monitor_agent(lat, lon)
        st.info("Auto-poll ran this refresh.")
        with st.expander("Auto-poll results"):
            st.json(results)

    st.divider()
    st.subheader("Topic: api_status (Tail view)")
    tail_msgs = read_last("api_status", n=tail_n)
    st.write(f"Showing last {len(tail_msgs)} messages")
    if tail_msgs:
        st.dataframe(
            [
                {
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(m["ts"])),
                    "api": m.get("api"),
                    "status": m.get("status"),
                    "latency_s": round(m.get("latency_s", 0.0), 3) if m.get("latency_s") is not None else None,
                    "temperature_c": m.get("temperature_c"),
                    "price_usd": m.get("price_usd"),
                    "ip": m.get("ip"),
                    "code": m.get("code"),
                    "error": m.get("error"),
                }
                for m in tail_msgs
            ],
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No messages yet. Click **POLL_APIS** to publish some.")

with right:
    st.subheader("Agent 2 — Kafka Status Agent")
    if st.button("ANALYZE_TOPIC"):
        analyzed = analyze_api_status(read_last("api_status", n=tail_n))
        st.success("Analysis complete.")
        st.json(analyzed)

        # Friendly highlights
        if analyzed["anomalies"]:
            st.warning("⚠️ Anomalies detected:")
            for a in analyzed["anomalies"]:
                st.write("-", json.dumps(a))
        else:
            st.success("No anomalies detected in the last window.")

    # Always show quick stats for convenience
    analyzed_quick = analyze_api_status(read_last("api_status", n=tail_n))
    with st.expander("Quick Summary (auto)"):
        st.json(analyzed_quick)

    st.divider()
    st.subheader("Export / Actions")
    # Export the whole topic log as JSON
    full_log = st.session_state.topics["api_status"]
    st.download_button(
        "EXPORT_LOG (Download JSON)",
        data=json.dumps(full_log, indent=2),
        file_name="api_status.json",
        mime="application/json",
    )

if show_actions:
    st.divider()
    st.subheader("Agentic Actions (Machine-readable)")
    st.code(json.dumps(actions_descriptor, indent=2), language="json")

st.caption(
    "Tech: Streamlit UI • requests for APIs • in-memory append-only log simulating Kafka topic. "
    "APIs used: Open-Meteo (weather), CoinDesk (BTC), IPify (IP)."
)
# --- Footer / Detailed Left Panel Explanation ---
st.markdown("---")
st.markdown(
    """
    ## 📘 Left Panel – Detailed Explanation

    ### 1) Latitude (°)
    - **What it controls:** Sets the geographic latitude used by the app for location-based features like weather, IoT, or geofencing.
    - **Valid range:** `-90.0` (South Pole) to `+90.0` (North Pole).
    - **Example:** `28.6139` for New Delhi.

    ### 2) Longitude (°)
    - **What it controls:** Sets the geographic longitude paired with the latitude.
    - **Valid range:** `-180.0` (west) to `+180.0` (east).
    - **Example:** `77.2090` for New Delhi.

    ### 3) Kafka Cluster Details
    - **Purpose:** Connects the app to a running Kafka cluster for streaming data.
    - **Fields:** Broker URL, authentication settings, topic name.
    - **Example:** `localhost:9092` for local testing.

    ### 4) API Credentials
    - **Purpose:** Stores secure API keys or tokens for external integrations.
    - **Example:** Weather API key, Kafka REST Proxy credentials.
    - **Security Tip:** Never hardcode secrets in public GitHub repos — use environment variables.

    ### 5) Topic Selection
    - **Purpose:** Chooses which Kafka topic data will be sent to or read from.
    - **Example:** `iot_sensor_stream` for IoT devices, `user_activity` for app logs.

    ### 6) Real-Time Control Options
    - **Purpose:** Control app behavior dynamically without redeploying.
    - **Examples:** Toggle simulation mode, choose refresh intervals, enable live dashboards.

    ### 7) Visual Grouping (Mental Model)
    - **Purpose:** Groups inputs in the sidebar to match real Kafka pipeline stages.
    - **Group Example:**
        - **Data Input Controls:** Latitude, Longitude.
        - **Kafka Connection:** Cluster details, credentials, topic.
        - **Output & Visualization:** Stream output toggles, chart settings.
    - **Benefit:** Helps non-technical users understand data flow visually.

    ---
    ✅ *This panel is designed for quick setup, testing, and demonstrating real Kafka architecture in action, even for MVPs.*
    """
)
