package ro.puk3p.sentinel.dataplatform.ingestion.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ingestion")
data class IngestionProperties(
    val backendBaseUrl: String = "http://localhost:8082",
    val pageSize: Int = 200,
    val pollIntervalMs: Long = 5000,
    val topics: Topics = Topics(),
) {
    data class Topics(
        val alerts: String = "ids.alerts",
        val traffic: String = "ids.traffic",
    )
}
