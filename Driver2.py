from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading
import socket
import sys

class ConsumeDriver(threading.Thread):
    def __init__(self, nodeID):
        self.nodeID = nodeID
        self.consumer = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
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
            print(message)
            print(self.configs)
            print("YES THE",self.configs[message["testID"]]["testType"],"TEST HAS STARTED FOR",message["testID"])
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
    
    def sendMetrics(self,mean,median,minn,maxx):
        data={
            "nodeID": self.nodeID,
            "test_id": 1,
            "metrics":{
                "mean_latency": mean,
                "median_latency": median,
                "min_latency": minn,
                "max_latency": maxx
            }
        }
        self.producer.send(topic='metrics', value=data)
        self.producer.flush()
        print("Metrics Sent")



if __name__ == "__main__":
    # while True:

    nodeID = sys.argv[1]
    P_driver = ProduceDriver(nodeID)
    C_driver = ConsumeDriver(nodeID)
    C_driver.initialize()
    # P_driver.sendHeartbeat("YES")
    # P_driver.sendMetrics(3,3,1,5)
    P_driver.producer.flush()
    P_driver.producer.close()