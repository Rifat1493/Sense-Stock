#!/usr/bin/env bash
/home/bdm/Downloads/kafka/bin/zookeeper-server-start.sh /home/bdm/Downloads/kafka/config/zookeeper.properties &
/home/bdm/Downloads/kafka/bin/kafka-server-start.sh /home/bdm/Downloads/kafka/config/server.properties &