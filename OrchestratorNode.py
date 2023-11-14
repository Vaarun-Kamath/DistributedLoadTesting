from kafka import KafkaConsumer, KafkaProducer
import json
import time

class OrchestratorNode:

    def __init__(self):
        self.producer = KafkaProducer()
        
    def sendTestConfig(self, testID, testType, testDelay):
        data = {
            "testID": testID, #! <RANDOMLY GENERATED UNQUE TEST ID>,
            "testType": testType,
            "testDelay": testDelay,
        }
        self.producer.send(topic='test_config', value=json.dumps(data).encode())
        self.producer.flush()
        print("Test configuration set")
    
    def triggerTest(self, testID):
        data = {
            "testID": testID, 
            "trigger": "YES"
        }
        self.producer.send(topic='trigger', value=json.dumps(data).encode())
        self.producer.flush()
        print("Trigger request sent")

orchestrator = OrchestratorNode()

testID = 'fnfn3221'
testType = 'Avalanche'
# testType = 'Tsunami'
if testType == 'Avalanche':
    testDelay = 0
else:
    testDelay = int(input("Enter delay (in ms): "))

orchestrator.sendTestConfig(testID,testType, testDelay)
orchestrator.triggerTest(testID)

orchestrator.producer.flush()
orchestrator.producer.close()