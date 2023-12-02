import streamlit as st
from kafka import KafkaConsumer
import json
import datetime as dt
import sys

consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))
consumer.subscribe(['metrics', 'heartbeat'])

st.title('Kafka Consumer Streamlit App')

min_value = float('inf')
max_value = float('-inf')
recent_latency = 0
median_latency = 0
mean_latency = 0
latencies = []

alive_nodes_ids = set()

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
    global alive_nodes_ids

    alive_nodes_ids.add(message_value["node_id"])

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
alive_nodes = st.text("")

# Line chart for latencies over time
latency_chart = st.line_chart()
bar_chart = st.bar_chart()

for msg in consumer:

    if msg is None:
        continue

    try:
        print(msg)

        if msg.topic == "test_info":
            handle_non_metrics(list(msg.values())[0][0].value)
            continue

        print("LAT: ",msg.value["metrics"]["latency"])
        message_value = msg.value["metrics"]["latency"]
        update_values(message_value)

        test_id.text("Test ID: "+str(msg.value['test_id']))
        recent_lat.text("Recent Latency: "+str(recent_latency))
        min_lat.text("Min Latency: "+str(min_value))
        max_lat.text("Max Latency: "+str(max_value))
        median_lat.text("Median Latency: "+str(median_latency))
        mean_lat.text("Mean Latency: "+str(mean_latency))
        alive_nodes.text("Alive Nodes: "+str(alive_nodes_ids))

        time_stamps.append(dt.datetime.now().timestamp())
        mean_latencies.append(mean_latency)
        median_latencies.append(median_latency)
        min_latencies.append(min_value)
        max_latencies.append(max_value)
        recent_latencies.append(recent_latency)

        latency_chart.line_chart({
            # 'Time Stamps': time_stamps,
            'Mean Latencies': mean_latencies,
            'Median Latencies': median_latencies,
            'Recent Latencies': recent_latencies,
            'Min Latencies': min_latencies,
            'Max Latencies': max_latencies
        })

        bar_chart.bar_chart({
            'Mean Latencies': mean_latency,
            'Median Latencies': median_latency,
            'Recent Latencies': recent_latency,
            'Min Latencies': min_value,
            'Max Latencies': max_value
        })



    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except KeyError as e:
        pass
    except Exception as e:
        pass

consumer.close()
