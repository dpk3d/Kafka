#!/usr/bin/env bash
set -x
set -e

# Create Topic
topicName=$1
retentionPeriod=${2:-3153000000}
# Replace xxx ==> with the ip Address of avaialble zookeper/bootstrap server
ZOOKEEPER_QUORUM=${ZOOKEEPER_QUORUM:- 192.xxx.xxx.xx:2181,192.xxx.xxx.xx:2181,192.xxx.xxx.xx:2181}
KAFKA_DIR=${KAFKA_DIR:-/mnt/disk/kafka/confluent-5.0.0}
echo "Creating Kafka Topic"
${KAFKA_DIR}/bin/kafka-topics --create --zookeeper ${ZOOKEEPER_QUORUM} --replication-factor 1 --partition 3 --topic $topicName --config retention.ms=${retentionPeriod}

# Other ways to create topic
# > bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic Deepak
# > bin/kafka-topics.sh --list --bootstrap-server localhost:9092
