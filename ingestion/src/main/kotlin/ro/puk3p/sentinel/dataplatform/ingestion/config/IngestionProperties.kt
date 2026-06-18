package ro.puk3p.sentinel.dataplatform.ingestion.config

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties(prefix = "ingestion")
data class IngestionProperties(
    val backendBaseUrl: String = "http://localhost:8082",
    // Edge-agent API key sent as X-API-Key when polling the backend's protected
    // /api/alerts endpoint (the platform auth lockdown made it require ROLE_AGENT).
    val apiKey: String = "",
    val pageSize: Int = 200,
    val pollIntervalMs: Long = 5000,
    val sendTimeoutSeconds: Long = 10,
    val watermarkFile: String = "data/alert-watermark",
    val topics: Topics = Topics(),
) {
    data class Topics(
        val alerts: String = "ids.alerts",
        val traffic: String = "ids.traffic",
    )
}
