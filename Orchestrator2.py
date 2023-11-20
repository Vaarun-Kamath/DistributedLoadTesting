from kafka import KafkaConsumer, KafkaProducer
import json
import time
import threading

class ProduceOrch(threading.Thread):

    def __init__(self):
        self.producer = KafkaProducer(value_serializer = lambda m: json.dumps(m).encode('ascii'))
        
    def sendTestConfig(self, testID, testType, testDelay):
        data = {
            "testID": testID, #! <RANDOMLY GENERATED UNQUE TEST ID>,
            "testType": testType,
            "testDelay": testDelay,
        }
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



class ConsumeOrch(threading.Thread):
    def __init__(self):
        self.consumer1 = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.consumer2 = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.consumer3 = KafkaConsumer(value_deserializer = lambda m: json.loads(m.decode('ascii')))
        self.metrics = []
    
    def initialize(self):
        global running
        self.consumer1.subscribe(['register'])
        n = 1 #! Number of drivers
        while running:
            msg = self.consumer1.poll(1000)
            if len(msg) > 0:
                print('--------------------------------')
                # print('Length of message: ',len(msg),'\n\n',type(msg),'\n\n',msg,"\n\n\n")
                print(f"Topic: {list(msg.values())[0][0].topic}")
                print(f"Message: {list(msg.values())[0][0].value}")
                print('--------------------------------')
                n -= 1

            if n == 0:
                self.consumer1.close() #! Safety Measure

                print('Consumer Closed')
                break
    
    def listenHeartbeat(self):
        self.consumer2.subscribe(['heartbeat'])
        n = 1
        while running:
            msg = self.consumer2.poll(1000)
            if len(msg) > 0:
                print('--------------------------------')
                # print('Length of message: ',len(msg),'\n\n',type(msg),'\n\n',msg,"\n\n\n")
                print(f"Topic: {list(msg.values())[0][0].topic}")
                print(f"Message: {list(msg.values())[0][0].value}")
                print('--------------------------------')
                n -= 1

            if n == 0:
                self.consumer2.close() #! Safety Measure
                print('Consumer Closed')
                break
    
    def getMetrics(self):
        self.consumer3.subscribe(['metrics', 'success'])
        print("Listening for metrics:")
        d_node_metrics = {}
        for msg in self.consumer3:
        # while running:
            # msg = self.consumer3.poll(100)
            # data = list(msg.values())[0][0].value 
            data = msg.value 
            print(data)
            if not running or ('status' in data and data['status'] == 'success'):
                break
            if len(msg) > 0:
                # print("msg: ", msg.value)
                # print(type(msg.value))
                # data = list(msg.values())[0][0].values()
                # print(f"Message: {data}")
                self.metrics.append(data)
                if data['node_id'] not in d_node_metrics:
                    d_node_metrics[data['node_id']] = 0
                d_node_metrics[data['node_id']] += 1
        print(f"Metrics Details: {d_node_metrics}")
        self.consumer3.close()


def consume():   
    global running
    print("Hello World")
    C_orchestrator = ConsumeOrch()
    C_orchestrator.initialize()
    C_orchestrator.getMetrics()
def produce():
    pass
    
def main():
    global running, tests, testID
    running = True
    tests=[]
    testID=None
    while running:
        # C_orchestrator = ConsumeOrch()
        # C_orchestrator.initialize()
        cons = threading.Thread(target=consume)
        cons.start()
        print("""Menu:
              1)Send Test Configurations
              2)Trigger a Test
              3)Stop Registering Driver Nodes
              4)Stop Testing
              5)Exit""")
        choice=int(input("Enter your choice: "))
        if(choice==1):  
            P_orchestrator = ProduceOrch()
            testID = input("Enter your test ID: ")
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
                
            P_orchestrator.sendTestConfig(testID, testType, testDelay)
        elif(choice==2):
            if(testID in tests):
                P_orchestrator.triggerTest(testID)
            else:
                print("The TEST-ID does not exist")
        elif(choice==3):
            pass
        elif(choice==4):
            break
        elif(choice==5):
            running = False

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