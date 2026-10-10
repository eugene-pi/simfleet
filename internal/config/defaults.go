package config

const ENV_WORKER_ID string = "WORKER_ID"
const ENV_DATABASE_URL string = "DATABASE_URL"
const ENV_SIMFLEET_LEASE_DURATION string = "SIMFLEET_LEASE_DURATION"
const ENV_SIMFLEET_BATCH_SIZE string = "SIMFLEET_BATCH_SIZE"
const ENV_SIMFLEET_WORKER_STAYS string = "SIMFLEET_WORKER_STAYS"

const batch_size_default int = 50
const lease_duration_sec_default int = 10
const renew_lease_coef int = 2
