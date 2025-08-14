# ─────────────────────────────────────────────────────────────────────────────
# File: app.py
# Description: Streamlit Kafka MVP with free/open-source "Agentic" monitor
#              Works on Streamlit Cloud (free) + external Kafka (e.g., Confluent Cloud free)
#              Uses only free & open-source libraries. LLM is optional via
#              Hugging Face Inference API (open models) with graceful fallback.
# ─────────────────────────────────────────────────────────────────────────────

import os
import json
import time
import typing as t
import requests
import streamlit as st

# We import confluent-kafka lazily after credentials are present

# --------------------------- UI / PAGE CONFIG -------------------------------
st.set_page_config(page_title="Kafka MVP + Agent Agent", page_icon="📬", layout="centered")
st.title("📬 Kafka MVP + 🧠 Agentic Monitor")
st.caption(
    "Free & open-source stack: Streamlit (UI) · Apache Kafka API (messaging) · optional open LLM via Hugging Face Inference API."
)

# ------------------------- SECRETS / ENV HANDLING ---------------------------
REQUIRED_KEYS = ["BOOTSTRAP_SERVERS", "API_KEY", "API_SECRET"]


def _available_secret_keys() -> t.List[str]:
    try:
        return list(st.secrets.keys())
    except Exception:
        return []


def _get_cred(key: str) -> t.Optional[str]:
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.environ.get(key) or st.session_state.get(key)


def _have_all_kafka_creds() -> bool:
    return all(bool(_get_cred(k)) for k in REQUIRED_KEYS)


with st.expander("Diagnostics (setup help)"):
    st.write("Secrets present:", _available_secret_keys())
    st.write({k: bool(_get_cred(k)) for k in REQUIRED_KEYS})

if not _have_all_kafka_creds():
    st.warning(
        "Kafka credentials not found in Streamlit **Secrets** / env. Paste them to run this session (temporary)."
    )
    with st.form("paste_creds_form"):
        bs = st.text_input("BOOTSTRAP_SERVERS", placeholder="pkc-xxxxx.region.confluent.cloud:9092")
        ak = st.text_input("API_KEY", type="password")
        sec = st.text_input("API_SECRET", type="password")
        if st.form_submit_button("Use for this session"):
            st.session_state["BOOTSTRAP_SERVERS"] = bs.strip()
            st.session_state["API_KEY"] = ak.strip()
            st.session_state["API_SECRET"] = sec.strip()
            if _have_all_kafka_creds():
                st.success("Session credentials set.")
            else:
                st.error("All three fields are required.")
    st.stop()

# ----------------------------- KAFKA CLIENTS --------------------------------
from confluent_kafka import Producer, Consumer, KafkaError, TopicPartition

BOOTSTRAP_SERVERS = _get_cred("BOOTSTRAP_SERVERS")
API_KEY = _get_cred("API_KEY")
API_SECRET = _get_cred("API_SECRET")

COMMON_CONF = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "PLAIN",
    "sasl.username": API_KEY,
    "sasl.password": API_SECRET,
}


def make_producer() -> Producer:
    return Producer(COMMON_CONF)


def make_consumer(group_id: str = "streamlit-group") -> Consumer:
    conf = dict(COMMON_CONF)
    conf.update({
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    return Consumer(conf)


# ------------------------------- UI CONTROLS --------------------------------
colA, colB = st.columns(2)
with colA:
    topic = st.text_input("Kafka topic", value="demo-topic")
with colB:
    group = st.text_input("Consumer group", value="streamlit-group")

st.subheader("Send a message")
user_msg = st.text_input("Message")
if st.button("Send to Kafka"):
    if user_msg.strip():
        try:
            p = make_producer()
            p.produce(topic, value=user_msg.encode("utf-8"))
            p.flush()
            st.success("✅ Message sent")
        except Exception as e:
            st.error(f"Failed to send: {e}")
    else:
        st.warning("Type a message first.")

st.subheader("Read messages (quick poll)")
if st.button("Read now"):
    try:
        c = make_consumer(group)
        c.subscribe([topic])
        messages = []
        empty_polls = 0
        while empty_polls < 2:  # poll briefly
            m = c.poll(1.0)
            if m is None:
                empty_polls += 1
                continue
            if m.error():
                if m.error().code() != KafkaError._PARTITION_EOF:
                    messages.append(f"[Kafka error] {m.error()}")
            else:
                payload = m.value().decode("utf-8") if m.value() else ""
                ts_type, ts_ms = m.timestamp()
                messages.append({
                    "partition": m.partition(),
                    "offset": m.offset(),
                    "timestamp_ms": ts_ms,
                    "value": payload,
                })
        c.close()
        if messages:
            st.write(messages)
        else:
            st.info("No new messages right now.")
    except Exception as e:
        st.error(f"Read failed: {e}")

# ------------------------- KAFKA METRICS SNAPSHOT ---------------------------

def get_topic_partitions(c: Consumer, tname: str) -> t.List[int]:
    md = c.list_topics(tname, timeout=5)
    if tname not in md.topics or md.topics[tname].error is not None:
        raise RuntimeError(f"Topic '{tname}' not found or metadata error.")
    return sorted(list(md.topics[tname].partitions.keys()))


def snapshot_kafka_status(tname: str, group_id: str) -> dict:
    """Collect a lightweight snapshot: partitions, watermarks, committed offsets,
    and a quick sample read to estimate recent rate. Designed for Streamlit free tier.
    """
    c = make_consumer(group_id)
    parts = get_topic_partitions(c, tname)

    status = {
        "topic": tname,
        "group": group_id,
        "partitions": [],
        "sample_messages": 0,
        "sample_window_sec": 0,
        "now_ms": int(time.time() * 1000),
    }

    # Watermarks & committed offsets
    tp_list = []
    for p_id in parts:
        tp = TopicPartition(tname, p_id)
        low, high = c.get_watermark_offsets(tp, timeout=5)
        committed = c.committed([TopicPartition(tname, p_id)], timeout=5)[0]
        committed_off = committed.offset if committed and committed.offset is not None else None
        status["partitions"].append({
            "partition": p_id,
            "low": int(low),
            "high": int(high),
            "committed": None if committed_off is None or committed_off < 0 else int(committed_off),
            "lag": None if committed_off is None or committed_off < 0 else int(max(high - committed_off, 0)),
        })
        tp_list.append(tp)

    # Quick sample poll to estimate recent traffic
    c.subscribe([tname])
    start = time.time()
    received = 0
    while time.time() - start < 2.0:  # 2 seconds sample
        m = c.poll(0.5)
        if m is None:
            continue
        if m.error():
            if m.error().code() != KafkaError._PARTITION_EOF:
                # count as an observation but don't crash
                pass
        else:
            received += 1
    c.close()

    status["sample_messages"] = received
    status["sample_window_sec"] = round(time.time() - start, 3)
    return status


st.subheader("Kafka Status Snapshot")
if st.button("🔎 Collect snapshot"):
    try:
        snap = snapshot_kafka_status(topic, group)
        st.json(snap)
    except Exception as e:
        st.error(f"Snapshot failed: {e}")

# --------------------------- AGENTIC EXPLAINER ------------------------------
# Open-source LLM via Hugging Face Inference API (optional)
# Model ideas (all open-weights):
#   - mistralai/Mistral-7B-Instruct-v0.2
#   - HuggingFaceH4/zephyr-7b-beta
#   - google/gemma-2-2b-it (license permits use; open weights)
# Provide HF_TOKEN (secret) and HF_MODEL_ID to enable. Fallback = rule-based.

HF_MODEL_ID = os.environ.get("HF_MODEL_ID") or st.secrets.get("HF_MODEL_ID", "") if "HF_MODEL_ID" in _available_secret_keys() else os.environ.get("HF_MODEL_ID")
HF_TOKEN = os.environ.get("HF_TOKEN") or (st.secrets.get("HF_TOKEN", "") if "HF_TOKEN" in _available_secret_keys() else os.environ.get("HF_TOKEN"))


def call_hf_inference(model: str, prompt: str, max_new_tokens: int = 256, temperature: float = 0.2) -> str:
    url = f"https://api-inference.huggingface.co/models/{model}"
    headers = {"Authorization": f"Bearer {HF_TOKEN}"} if HF_TOKEN else {}
    payload = {
        "inputs": prompt,
        "parameters": {"max_new_tokens": max_new_tokens, "temperature": temperature, "return_full_text": False},
        "options": {"wait_for_model": True},
    }
    r = requests.post(url, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    # HF returns list of dicts with 'generated_text'
    if isinstance(data, list) and data and "generated_text" in data[0]:
        return data[0]["generated_text"].strip()
    # Some models return dict
    if isinstance(data, dict) and "generated_text" in data:
        return data["generated_text"].strip()
    return str(data)


def rule_based_agent_explanation(snap: dict) -> str:
    parts = snap.get("partitions", [])
    total_lag = sum(p.get("lag") or 0 for p in parts)
    high_sum = sum(p.get("high") or 0 for p in parts)
    committed_missing = [p["partition"] for p in parts if p.get("committed") is None]
    msg_rate = 0.0
    if snap.get("sample_window_sec"):
        msg_rate = snap.get("sample_messages", 0) / max(snap.get("sample_window_sec"), 1e-6)

    hints = []
    if total_lag > 0:
        hints.append(f"Consumer group has total lag {total_lag}. Consider scaling consumers or checking processing speed.")
    if committed_missing:
        hints.append(
            "No committed offsets for partitions: " + ", ".join(map(str, committed_missing)) + 
            ". This may mean the group hasn't started consuming or auto-commit is disabled."
        )
    if high_sum == 0:
        hints.append("Topic appears empty. Produce a few test messages.")
    if msg_rate < 0.5:
        hints.append("Low message rate in the last 2s sample. If unexpected, verify producers.")
    if not hints:
        hints.append("Kafka looks healthy: offsets advance and lag is near zero.")

    return (
        f"Topic '{snap.get('topic')}' for group '{snap.get('group')}'.\n"
        f"Partitions: {len(parts)}; Total lag: {total_lag}; Sample rate: {msg_rate:.2f} msg/s.\n"
        + "\n- " + "\n- ".join(hints)
    )


st.subheader("🧠 Run Kafka Agent (open-source model optional)")
st.caption("If you provide HF_TOKEN + HF_MODEL_ID (e.g., mistralai/Mistral-7B-Instruct-v0.2), the agent will use an open LLM. Otherwise it uses a lightweight rule-based explainer.")

agent_run = st.button("Run Agent on latest snapshot")

if agent_run:
    try:
        snap = snapshot_kafka_status(topic, group)
        st.json(snap)
        explanation = None
        if HF_TOKEN and HF_MODEL_ID:
            prompt = (
                "You are a helpful SRE agent monitoring Apache Kafka. "
                "Given the JSON snapshot, explain the current status in concise plain English, "
                "call out risks (consumer lag, missing commits, empty topic), and suggest 3 concrete actions.\n\n"
                f"JSON:\n{json.dumps(snap, indent=2)}\n\n"
                "Return a short markdown report with headings: Status, Findings, Recommendations."
            )
            try:
                explanation = call_hf_inference(HF_MODEL_ID, prompt)
            except Exception as e:
                explanation = f"(LLM call failed; falling back)\n\n{rule_based_agent_explanation(snap)}\n\nError: {e}"
        else:
            explanation = rule_based_agent_explanation(snap)
        st.markdown(explanation)
    except Exception as e:
        st.error(f"Agent run failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# File: requirements.txt (place this in your repo root)
# ─────────────────────────────────────────────────────────────────────────────
#
# streamlit
# confluent-kafka
# requests
#
# (All are free & open-source.)
#
# If you plan to use dotenv locally, you can add:
# python-dotenv
# ─────────────────────────────────────────────────────────────────────────────
