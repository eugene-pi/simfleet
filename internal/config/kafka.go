package config

import (
	"strings"
)

type KafkaConfig struct {
	Brokers    []string
	Group      string
	Partitions int
}

func LoadKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers:    strings.Split(loadString(ENV_KAFKA_BROKERS, kafka_broker_default), ","),
		Group:      loadString(ENV_KAFKA_GROUP, kafka_group_default),
		Partitions: loadInt(ENV_KAFKA_PARTITIONS, kafka_partitions_default),
	}
}
