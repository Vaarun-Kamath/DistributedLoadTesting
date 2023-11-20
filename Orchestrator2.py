from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading
import uuid
import sys
import statistics

# C_orchestrator = None

all_metrics = {}

class ConsumeOrch(threading.Thread):
    def __init__(self):
        self.consumer1 = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.consumer2 = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.consumer3 = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.metrics = []
        self.numDrivers = 0
        self.driverNodes = {}
    
    def initialize(self):
        global running
        self.consumer1.subscribe(['register'])

        def handle_driver_node(nodeID):
            while True:
                # print(f"{nodeID} 'heart check")
                if not self.driverNodes[nodeID]:
                    print(f"Driver node {nodeID} has died!")
                    del self.driverNodes[nodeID]
                    break
                self.driverNodes[nodeID]=0
                time.sleep(2)
        
        # n = self.numDrivers #! Number of drivers
        while running:
            msg = self.consumer1.poll(100)
            if len(msg) > 0:
                self.numDrivers += 1
                # print('\n--------------------------------')
                # print('Length of message: ',len(msg),'\n\n',type(msg),'\n\n',msg,"\n\n\n")
                node = list(msg.values())[0][0].value
                # print(f"Topic: {list(msg.values())[0][0].topic}")
                # print(f"Message: {node}")

                self.driverNodes[node["node_id"]] = 1
                print(f'Node {node["node_id"]} has connected!')
                threading.Thread(target=handle_driver_node, args=(node["node_id"],), daemon=True).start()
                # print('--------------------------------')
                # n -= 1

            # if n == 0:
        self.consumer1.close() #! Safety Measure
        print('Register Consumer Closed')
    
    def listenHeartbeat(self):
        # c=0
        self.consumer2.subscribe(['heartbeat'])
        while running:
            msg = self.consumer2.poll(1000)
            if len(msg) > 0:
                # print("Heart has Beat")
                # print('--------------------------------')
                # print('Length of message: ',len(msg),'\n\n',type(msg),'\n\n',msg,"\n\n\n")
                node = list(msg.values())[0][0].value
                # print(f"Topic: {list(msg.values())[0][0].topic}")
                # print(f"Message: {node}")
                # print('--------------------------------')
                self.driverNodes[node["node_id"]] = 1
                # c+=1
                # if(c>(len(self.driverNodes)*2)):
                #     print(self.driverNodes)
                #     c=0

        self.consumer2.close() #! Safety Measure
        print('HeartBeat Consumer Closed')
    
    def getMetrics(self):
        self.consumer3.subscribe(['metrics'])
        global all_metrics
        

        print("Listening for metrics:")
        d_node_metrics = {}
        completed_drivers = 0
        

        latencies = []

        for msg in self.consumer3:
            # print("MESSG RECIEVED ")
        # while running:
        #     msg = self.consumer3.poll(1000)
            # print("msg: ",msg)
            data = msg.value
            # print(data)
            if len(msg) > 0:
                test=data["test_id"]

                node=data["node_id"]

                if node not in all_metrics[test]:
                    all_metrics[test][node] = {}

                metrics=data["metrics"]
                all_metrics[test][node] = metrics
                latencies.append(metrics["latency"])


                all_metrics[test]["Aggregate"]["min_latency"] = min(latencies)
                all_metrics[test]["Aggregate"]["max_latency"] = max(latencies)
                all_metrics[test]["Aggregate"]["mean_latency"] = statistics.mean(latencies)
                all_metrics[test]["Aggregate"]["median_latency"] = statistics.median(latencies)
                
                        

                self.metrics.append(data)
                if data['node_id'] not in d_node_metrics:
                    d_node_metrics[data['node_id']] = 0
                d_node_metrics[data['node_id']] += 1
            if data['end']:
                completed_drivers += 1
                print(f"Test Done for {data['node_id']}")
                print(completed_drivers, len(self.driverNodes))
                if completed_drivers % len(self.driverNodes) == 0: # Test ended
                    print(f"All Metrics Details: {all_metrics}")
                    print("*"*30)
                    print(f"Recent Metrics Details for {test}: {all_metrics[test]}")
                    self.metrics = []
                    latencies = []

        self.consumer3.close()


class ProduceOrch(threading.Thread):

    def __init__(self):
        self.producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
        # self.C_orchestrator = C_orchestrator
        
    def sendTestConfig(self, testID, testType, testDelay, reqs):
        data = {
            "testID": testID, #! <RANDOMLY GENERATED UNQUE TEST ID>,
            "testType": testType,
            "testDelay": testDelay,
            "requests": reqs 
        }
        global all_metrics
        all_metrics[testID] = {"Aggregate": {}}
        self.producer.send(topic='test_config', value=data)
        self.producer.flush()
        print("Test configuration set")
    
    def triggerTest(self, testID):
        data = {
            "testID": testID, 
            "trigger": "YES"
        }
        self.producer.send(topic='trigger', value=data)
        self.producer.flush()
        print("Trigger request sent")

def consumeInit(C_orchestrator):   
    global running
    print("Hello World")

    C_orchestrator.initialize()

def consumeMetrics(C_orchestrator):
    C_orchestrator.getMetrics()

def consume_heart_beat(C_orchestrator):
    C_orchestrator.listenHeartbeat()

def main():
    global running, tests, testID
    running = True
    tests=[]
    testID=None
    C_orchestrator = ConsumeOrch()
    # numDrivers = int(input("Number of driver nodes: "))
    cons = threading.Thread(target=consumeInit, args=(C_orchestrator,), daemon=True) # Regestring
    cons.start()
    
    metr = threading.Thread(target=consumeMetrics, args=(C_orchestrator,), daemon=True) # metrics
    metr.start()


    heart_beat_thread = threading.Thread(target=consume_heart_beat, args=(C_orchestrator,), daemon=True) # metrics
    heart_beat_thread.start()


    
    while running:
        # C_orchestrator = ConsumeOrch()
        # C_orchestrator.initialize()
        
        print("""Menu:
              1)Send Test Configurations
              2)Trigger a Test
              3)Stop Registering Driver Nodes
              4)Stop Testing
              5)Exit""")
        choice=int(input("Enter your choice:\n"))
        if(choice==1):  
            P_orchestrator = ProduceOrch()
            testID = str(uuid.uuid4())
            # 'fnfn3221'
            tests.append(testID)
            testType = int(input("""Select Type of Testing:
                             1) Avalanche
                             2) Tsunami
                             """))

            if(testType == 1 ):
                testType="Avalanche"
                testDelay = 0
            elif testType == 2:
                testType="Tsunami"
                testDelay = int(input("Enter delay (in ms): "))
                
            num_reqs = int(input("Enter number of requests for this test: "))
            P_orchestrator.sendTestConfig(testID, testType, testDelay, num_reqs)
        elif(choice==2):
            if(testID in tests):
                P_orchestrator.triggerTest(testID)
                # metr.join()
            else:
                print("The TEST-ID does not exist")
        elif(choice==3):
            pass
        elif(choice==4):
            break
        elif(choice==5):
            running = False
            # quit()
            sys.exit()

if __name__ == '__main__':
    # run_it = threading.Thread(target=run)

    main()
            



    # C_orchestrator = ConsumeOrch()
    # C_orchestrator.initialize()
    # P_orchestrator = ProduceOrch()
    # testID = 'fnfn3221'
    # testType = 'Avalanche'
    # # testType = 'Tsunami'
    # if testType == 'Avalanche':
    #     testDelay = 0
    # else:
    #     testDelay = int(input("Enter delay (in ms): "))

    # P_orchestrator.sendTestConfig(testID,testType, testDelay)
    # P_orchestrator.triggerTest(testID)

    # P_orchestrator.producer.flush()
    # P_orchestrator.producer.close()

    # C_orchestrator.listenHeartbeat()
    # C_orchestrator.getMetrics()