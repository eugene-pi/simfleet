package config

const ENV_WORKER_ID string = "WORKER_ID"
const ENV_DATABASE_URL string = "DATABASE_URL"
const ENV_SIMFLEET_LEASE_DURATION string = "SIMFLEET_LEASE_DURATION"
const ENV_SIMFLEET_BATCH_SIZE string = "SIMFLEET_BATCH_SIZE"
const ENV_SIMFLEET_WORKER_STAYS string = "SIMFLEET_WORKER_STAYS"
const ENV_KAFKA_BROKERS string = "KAFKA_BROKERS"
const ENV_KAFKA_GROUP string = "KAFKA_GROUP"
const ENV_KAFKA_PARTITIONS string = "KAFKA_PARTITIONS"

const batch_size_default int = 50
const lease_duration_sec_default int = 10
const renew_lease_coef int = 2

const kafka_broker_default string = "localhost:9092"
const kafka_group_default string = "simfleet-workers"
const kafka_partitions_default int = 12
