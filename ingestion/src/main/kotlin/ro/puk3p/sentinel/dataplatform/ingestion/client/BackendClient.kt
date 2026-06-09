package ro.puk3p.sentinel.dataplatform.ingestion.client

import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties

@Component
class BackendClient(
    props: IngestionProperties,
) {
    private val web = WebClient.create(props.backendBaseUrl)
    private val pageSize = props.pageSize

    /**
     * Fetches the most recent alerts (newest first) as the raw ApiResponse map.
     * Incremental filtering is done client-side by the poller (watermark on
     * timestamp), which avoids the backend's server-side `from` filter.
     */
    @Suppress("UNCHECKED_CAST")
    fun fetchLatestAlerts(): Map<String, Any?>? =
        web.get()
            .uri { b ->
                b.path("/api/alerts")
                    .queryParam("size", pageSize)
                    .queryParam("sortBy", "timestamp")
                    .queryParam("direction", "desc")
                    .build()
            }
            .retrieve()
            .bodyToMono(Map::class.java)
            .block() as Map<String, Any?>?
}
