package ro.puk3p.sentinel.dataplatform.streaming.config

data class JobConfig(
    val bootstrapServers: String,
    val alertsTopic: String,
    val anomaliesTopic: String,
    val groupId: String,
    val windowSeconds: Long,
    val alertThreshold: Int,
    val checkpointIntervalMs: Long,
    val checkpointDir: String,
) {
    companion object {
        fun fromEnv(): JobConfig =
            JobConfig(
                bootstrapServers = env("KAFKA_BOOTSTRAP", "localhost:9092"),
                alertsTopic = env("ALERTS_TOPIC", "ids.alerts"),
                anomaliesTopic = env("ANOMALIES_TOPIC", "traffic-anomalies"),
                groupId = env("KAFKA_GROUP_ID", "flink-anomaly-detector"),
                windowSeconds = (env("WINDOW_SECONDS", "60").toLongOrNull() ?: 60L).coerceAtLeast(1L),
                alertThreshold = (env("ALERT_THRESHOLD", "3").toIntOrNull() ?: 3).coerceAtLeast(1),
                checkpointIntervalMs = (env("CHECKPOINT_INTERVAL_MS", "10000").toLongOrNull() ?: 10000L).coerceAtLeast(1000L),
                checkpointDir = env("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints/sentinel-anomaly-detector"),
            )

        private fun env(key: String, default: String): String =
            System.getenv(key)?.takeIf { it.isNotBlank() } ?: default
    }
}
