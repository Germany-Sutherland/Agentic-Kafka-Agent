import streamlit as st
from confluent_kafka import Producer, Consumer, KafkaError
import os

# Load credentials from Streamlit secrets
BOOTSTRAP_SERVERS = st.secrets["BOOTSTRAP_SERVERS"]
API_KEY = st.secrets["API_KEY"]
API_SECRET = st.secrets["API_SECRET"]
TOPIC = "demo-topic"

conf_producer = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET
}

conf_consumer = conf_producer.copy()
conf_consumer.update({
    'group.id': 'streamlit-group',
    'auto.offset.reset': 'earliest'
})

producer = Producer(conf_producer)
consumer = Consumer(conf_consumer)
consumer.subscribe([TOPIC])

st.title("📬 Kafka MVP Demo")

msg = st.text_input("Enter message:")

if st.button("Send to Kafka"):
    producer.produce(TOPIC, value=msg)
    producer.flush()
    st.success("Message sent!")

if st.button("Read messages"):
    messages = []
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                st.error(msg.error())
        else:
            messages.append(msg.value().decode('utf-8'))
    st.write(messages)
