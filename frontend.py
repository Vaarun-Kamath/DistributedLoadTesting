import streamlit as st
from kafka import KafkaConsumer
import json
import datetime as dt
import sys

consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))
consumer.subscribe(['metrics', 'test_info'])

st.title('Kafka Consumer Streamlit App')

min_value = float('inf')
max_value = float('-inf')
recent_latency = 0
median_latency = 0
mean_latency = 0
latencies = []

node_health = {}
requests_completed = 0

# Lists to store historical data for the line chart
time_stamps = []
mean_latencies = []
median_latencies = []
min_latencies = []
max_latencies = []
recent_latencies = []

def update_values(message_value):
    global min_value, max_value, recent_latency, median_latency, mean_latency, latencies

    latencies.append(message_value)
    recent_latency = message_value
    min_value = min(min_value, message_value)
    max_value = max(max_value, message_value)
    median_latency = sorted(latencies)[len(latencies) // 2]
    mean_latency = sum(latencies) / len(latencies)

def handle_non_metrics(message_value):
    global node_health, requests_completed
    
    node_health[message_value["node_id"]] = "ALIVE"
    requests_completed = message_value["requests_completed"]

# running = True
# stop_button = st.button("Stop Streamlit and Kafka Consumer")
# if stop_button:
#     # st.experimental_rerun()
#     running = False
#     sys.exit()

test_id = st.text("")
recent_lat = st.text("")
min_lat = st.text("")
max_lat = st.text("")
median_lat = st.text("")
mean_lat = st.text("")

# Line chart for latencies over time
latency_chart = st.line_chart()

for msg in consumer:

    if msg is None:
        continue

    try:
        print(msg)

        if msg.topic == "test_info":
            handle_non_metrics(msg.value)
            continue

        print("LAT: ",msg.value["metrics"]["latency"])
        message_value = msg.value["metrics"]["latency"]

        update_values(message_value)

        # Update the display values
        test_id.text("Test ID: "+str(msg.value['test_id']))
        recent_lat.text("Recent Latency: "+str(recent_latency))
        min_lat.text("Min Latency: "+str(min_value))
        max_lat.text("Max Latency: "+str(max_value))
        median_lat.text("Median Latency: "+str(median_latency))
        mean_lat.text("Mean Latency: "+str(mean_latency))

        # Append data to the historical lists for the line chart
        time_stamps.append(dt.datetime.now().timestamp())
        mean_latencies.append(mean_latency)
        median_latencies.append(median_latency)
        min_latencies.append(min_value)
        max_latencies.append(max_value)
        recent_latencies.append(recent_latency)

        # Update the line chart with the historical data
        latency_chart.line_chart({
            # 'Time Stamps': time_stamps,
            'Mean Latencies': mean_latencies,
            'Median Latencies': median_latencies,
            'Recent Latencies': recent_latencies,
            'Min Latencies': min_latencies,
            'Max Latencies': max_latencies
        })

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except KeyError as e:
        pass
    except Exception as e:
        pass

consumer.close()
