package config

import (
	"strings"
)

type KafkaConfig struct {
	Brokers    []string
	Group      string
	Topic      string
	Partitions int
	BatchSize  int
}

func LoadKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers:    strings.Split(loadString(ENV_KAFKA_BROKERS, kafka_broker_default), ","),
		Group:      loadString(ENV_KAFKA_GROUP, kafka_group_default),
		Topic:      loadString(ENV_KAFKA_TOPIC, kafka_topic_default),
		Partitions: loadInt(ENV_KAFKA_PARTITIONS, kafka_partitions_default),
		BatchSize:  loadInt(ENV_SIMFLEET_BATCH_SIZE, batch_size_default),
	}
}
