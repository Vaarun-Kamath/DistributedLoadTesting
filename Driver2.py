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

class ConsumeDriver(threading.Thread):
    def __init__(self, nodeID):
        self.nodeID = nodeID
        self.consumer = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
        host = socket.gethostname()
        ip = socket.gethostbyname(host)
        data={
            "nodeID": self.nodeID,
            "node_IP": ip,
            "message_type": "Driver Node Register"
        }
        self.producer.send(topic='register', value=data)
        self.producer.flush()
        print("Registered Driver Node")
        self.configs = {}

    def initialize(self):
        self.consumer.subscribe(['test_config','trigger'])
        # n = 2
        while True:
            msg = self.consumer.poll(1000)
            if len(msg) > 0:
                # print('--------------------------------')
                # print('Length of message: ',len(msg),'\n\n',type(msg),'\n\n',msg,"\n\n\n") 
                topic=list(msg.values())[0][0].topic
                message=list(msg.values())[0][0].value
                print(f"Topic: {topic}")
                print(f"Message: {message}")
                print('--------------------------------')
                if(topic=="test_config"):
                    # print(type(message))
                    self.configs[message["testID"]]={"testType":message["testType"],"testDelay":message["testDelay"]}
                elif(topic=="trigger"):
                    self.run_test(message)

    def run_test(self,message):
        if(message["testID"] in self.configs.keys()):
            print("Configs: ",self.configs)
            testType = self.configs[message["testID"]]["testType"]
            testDelay = self.configs[message["testID"]]["testDelay"]
            print(testType,"test has started for testID",message["testID"])
            x = 5
            driver_side_latency = []
            for i in range(x):
                
                # driver_side["entry"].append(dt.datetime.now().microsecond/(10**3))
                entry = dt.datetime.now().timestamp()
                response = requests.get(url="http://localhost:5000/ping", params={"entry_driver": time.monotonic_ns()/(10**6)})
                if response:
                    print(f"{i+1} Recieved")
                exit = dt.datetime.now().timestamp()


                driver_side_latency.append((exit-entry)/(10**6))
                data = {
                    "node_id": self.nodeID,
                    "test_id": message["testID"],
                    "report_id": str(uuid.uuid4()),
                    "metrics": {
                        "mean_latency": sum(driver_side_latency)/len(driver_side_latency),
                        "median_latency": statistics.median(driver_side_latency),
                        "min_latency": max(driver_side_latency),
                        "max_latency": min(driver_side_latency)
                    }
                }
                self.producer.send(topic='metrics',value=data)
                time.sleep(testDelay/1000)
            print("Sending Success message!")
            self.producer.send(topic='success', value={"status":'success'})
        else:
            print("Invalid Test ID: ")
    


class ProduceDriver(threading.Thread):
    def __init__(self,nodeID):
        self.nodeID = nodeID
        self.producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
        host = socket.gethostname()
        ip = socket.gethostbyname(host)
        data={
            "nodeID": self.nodeID,
            "node_IP": ip,
            "message_type": "Driver Node Register"
        }
        self.producer.send(topic='register', value=data)
        self.producer.flush()
        print("Registered Driver Node")

    def sendHeartbeat(self, hb):
        data={
            "nodeID": self.nodeID,
            "heartbeat": hb
        }
        self.producer.send(topic='heartbeat', value=data)
        self.producer.flush()
        print("Heartbeat Sent")
    
    def sendMetrics(self, mean, median, min, maxx):
        data={
            "nodeID": self.nodeID,
            "test_id": 1,
            "metrics":{
                "mean_latency": mean,
                "median_latency": median,
                "min_latency": min,
                "max_latency": maxx
            }
        }
        self.producer.send(topic='metrics', value=data)
        self.producer.flush()
        print("Metrics Sent")



if __name__ == "__main__":
    # while True:

    nodeID = sys.argv[1]
    # P_driver = ProduceDriver(nodeID)
    C_driver = ConsumeDriver(nodeID)
    C_driver.initialize()
    C_driver.producer.flush()
    C_driver.producer.close()
    # P_driver.sendHeartbeat("YES")
    # P_driver.sendMetrics(3,3,1,5)
    # P_driver.producer.flush()
    # P_driver.producer.close()