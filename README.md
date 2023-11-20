# DistributedLoadTesting
 Temporary load testing bee dee project


## start
### For Windows
go to kafka folder
- start zookeeper
    ```
    bin\windows\zookeeper-server-start.bat config\zookeeper.properties
    ```
- start kafka
    ```
    bin\windows\kafka-server-start.bat config\server.properties
    ```

## stop
### For Windows
go to kafka folder
- stop kafka:
    ```
    bin\windows\kafka-server-stop.bat
    ```
- stop zookeeper
    ```
    bin\windows\zookeeper-server-stop.bat
    ```