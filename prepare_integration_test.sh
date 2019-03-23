#!/usr/bin/env bash
kafka-topics --create --topic topicnamestrategy --zookeeper localhost:2181 --partitions 1 --replication-factor 1
kafka-topics --create --topic recordnamestrategy --zookeeper localhost:2181 --partitions 1 --replication-factor 1
kafka-topics --create --topic topicrecordnamestrategy --zookeeper localhost:2181 --partitions 1 --replication-factor 1