package ro.puk3p.sentinel.dataplatform.ingestion.client

import com.fasterxml.jackson.databind.JsonNode
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties
import java.time.Instant

@Component
class BackendClient(
    props: IngestionProperties,
    builder: WebClient.Builder,
) {
    private val web = builder.baseUrl(props.backendBaseUrl).build()
    private val pageSize = props.pageSize

    /** Fetches alerts created at/after [from] (ascending), as the raw ApiResponse JSON. */
    fun fetchAlertsSince(from: Instant?): JsonNode? =
        web.get()
            .uri { b ->
                b.path("/api/alerts")
                    .queryParam("size", pageSize)
                    .queryParam("sortBy", "timestamp")
                    .queryParam("direction", "asc")
                if (from != null) {
                    b.queryParam("from", from.toString())
                }
                b.build()
            }
            .retrieve()
            .bodyToMono(JsonNode::class.java)
            .block()
}
