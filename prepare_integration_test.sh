#!/usr/bin/env bash
docker exec broker kafka-topics --create --topic topicnamestrategy --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec broker kafka-topics --create --topic recordnamestrategy --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec broker kafka-topics --create --topic topicrecordnamestrategy --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1