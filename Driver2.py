from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading
import socket
import sys
import requests
import datetime as dt
import statistics
import uuid

polling_rate = 1000
running = True
configs = {}
heart_beat_msg = {
    "node_id": nodeID,
    "heartbeat": "YES",
    "timestamp": dt.datetime.now().timestamp()
}

def initialize():
    global configs, consumer
    consumer.subscribe(['test_config','trigger'])
    while True:
        msg = consumer.poll(polling_rate)
        if not len(msg): continue
        print('--------------------------------')
        topic=list(msg.values())[0][0].topic
        message=list(msg.values())[0][0].value
        print(f"Topic: {topic}")
        print(f"Message: {message}")
        print('--------------------------------')
        if(topic == "test_config"):
            # print(type(message))
            configs[message["testID"]]={"testType":message["testType"],"testDelay":message["testDelay"]}
        elif(topic == "trigger"):
            run_test(message)

def run_test(message):
    global nodeID, configs, producer
    if(message["testID"] not in configs.keys()):
        print("Invalid Test ID: ")
        return
    
    print("Configs: ",configs)
    testType = configs[message["testID"]]["testType"]
    testDelay = configs[message["testID"]]["testDelay"]
    print(testType,"test has started for testID",message["testID"])
    x = 5
    driver_side_latency = []
    for i in range(x):
        
        # driver_side["entry"].append(dt.datetime.now().microsecond/(10**3))
        entry = dt.datetime.now().timestamp()
        response = requests.get(url="http://localhost:5000/ping", params={"entry_driver": time.monotonic_ns()/(10**6)})
        exit = dt.datetime.now().timestamp()
        # response = response.json()
        if response:
            print(f"{i+1} Recieved {response}")
        
        driver_side_latency.append((exit-entry)/(10**6))
        data = {
            "node_id": nodeID,
            "test_id": message["testID"],
            "report_id": str(uuid.uuid4()),
            "metrics": {
                "mean_latency": sum(driver_side_latency)/len(driver_side_latency),
                "median_latency": statistics.median(driver_side_latency),
                "min_latency": max(driver_side_latency),
                "max_latency": min(driver_side_latency)
            },
            "end": bool(i+1 == x)
        }
        print(data)
        print("\n--------\n")
        producer.send(topic='metrics',value=data)
        time.sleep(testDelay/1000)
    print("Sending Success message!")
    # producer.send(topic='success', value={"status":'success'})


def beat_heart():
    while running:
        heart_beat_msg["timestamp"] = dt.datetime.now().timestamp()
        producer.send(topic="heartbeat", value=heart_beat_msg)
        time.sleep(1)

def main():
    global nodeID, configs, consumer, producer
    nodeID = sys.argv[1]
    consumer = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
    producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))

    host = socket.gethostname()
    ip = socket.gethostbyname(host)
    data={
        "nodeID": nodeID,
        "node_IP": ip,
        "message_type": "Driver Node Register"
    }

    producer.send(topic='register', value=data)
    producer.flush()
    print("Registered Driver Node")
    heart_beat_thread = threading.Thread(target=beat_heart, daemon=True)
    heart_beat_thread.start()

    # main_thread = threading.Thread(target=initialize)
    # main_thread.start()
    initialize()

    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()