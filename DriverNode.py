from kafka import KafkaConsumer, KafkaProducer
import json
import time


class DriverNode:
    def __init__(self, nodeID):
        self.nodeID = nodeID
        self.consumer = KafkaConsumer()

    def initialize(self):
        self.consumer.subscribe(['test_config', 'trigger'])
        n = 2
        while True:
            msg = self.consumer.poll(1000)
            if len(msg) > 0:
                print('--------------------------------')
                # print('Length of message: ',len(msg),'\n\n',type(msg),'\n\n',msg,"\n\n\n")
                print(f"Topic: {list(msg.values())[0][0].topic}")
                print(f"Message: {list(msg.values())[0][0].value.decode()}")
                print('--------------------------------')
                n -= 1
                # data = json.loads(msg.values().decode('utf-8'))
                # print(data)
            if n == 0:
                self.consumer.close() #! Safety Measure
                print('Consumer Closed')
                break
            # Start thread
        

if __name__ == "__main__":
    nodeID = '302A'

    driver = DriverNode(nodeID)
    driver.initialize()