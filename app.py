import streamlit as st
from kafka import KafkaConsumer
import json
import threading
import time

st.set_page_config(page_title="Kafka MVP + Agentic AI", layout="wide")

# -------------------
# DEMO MODE SETTINGS
# -------------------
DEMO_BOOTSTRAP = "kafka.lenses.io:9092"
DEMO_TOPIC = "public-demo"

# Rule-based AI agent to "explain" Kafka stream patterns
def ai_agent_analysis(messages):
    if not messages:
        return "No recent messages. Kafka topic seems idle."
    if len(messages) > 20:
        return "High activity detected — possible spike in message traffic."
    if any("error" in m.lower() for m in messages):
        return "Alert: Some messages indicate errors."
    return "Message flow appears normal."

# Stream Kafka messages in background
def consume_kafka(bootstrap, topic, messages, stop_event, username=None, password=None):
    try:
        if username and password:
            sasl_conf = {
                'security_protocol': 'SASL_PLAINTEXT',
                'sasl_mechanism': 'PLAIN',
                'sasl_plain_username': username,
                'sasl_plain_password': password
            }
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                **sasl_conf,
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )
        else:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap,
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='latest',
                consumer_timeout_ms=1000
            )

        while not stop_event.is_set():
            for msg in consumer:
                if stop_event.is_set():
                    break
                messages.append(msg.value)
                if len(messages) > 50:  # keep memory small
                    messages.pop(0)
            time.sleep(0.5)
    except Exception as e:
        st.error(f"Kafka connection failed: {e}")

# -------------------
# Streamlit UI
# -------------------
st.title("Kafka MVP + Agentic AI Monitor")
st.markdown("Monitor Kafka topics in real-time with a lightweight free AI explainer.")

mode = st.radio("Choose Mode", ["Demo Mode (Public Kafka)", "Custom Kafka"])

if mode == "Custom Kafka":
    bootstrap = st.text_input("Bootstrap Servers", value="pkc-xxxxx.region.confluent.cloud:9092")
    topic = st.text_input("Topic", value="test-topic")
    api_key = st.text_input("API Key", type="password")
    api_secret = st.text_input("API Secret", type="password")
else:
    bootstrap = DEMO_BOOTSTRAP
    topic = DEMO_TOPIC
    api_key = None
    api_secret = None

if st.button("Start Monitoring"):
    messages = []
    stop_event = threading.Event()
    thread = threading.Thread(
        target=consume_kafka,
        args=(bootstrap, topic, messages, stop_event, api_key, api_secret),
        daemon=True
    )
    thread.start()

    placeholder = st.empty()
    analysis_placeholder = st.empty()

    try:
        while True:
            placeholder.table(messages[-10:])
            analysis = ai_agent_analysis(messages)
            analysis_placeholder.info(f"🤖 AI Agent Insight: {analysis}")
            time.sleep(2)
    except KeyboardInterrupt:
        stop_event.set()
