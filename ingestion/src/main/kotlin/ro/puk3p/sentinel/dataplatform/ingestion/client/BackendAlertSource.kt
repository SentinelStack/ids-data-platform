package ro.puk3p.sentinel.dataplatform.ingestion.client

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties

@Component
class BackendAlertSource(
    props: IngestionProperties,
) : AlertSource {
    private val log = LoggerFactory.getLogger(javaClass)
    private val web = WebClient.create(props.backendBaseUrl)
    private val pageSize = props.pageSize

    override fun fetchLatestAlerts(): List<Map<String, Any?>> {
        val body =
            try {
                requestLatest()
            } catch (ex: Exception) {
                log.warn("alert fetch failed: {}", ex.message)
                return emptyList()
            }
        return extractContent(body)
    }

    private fun requestLatest(): Map<*, *>? =
        web.get()
            .uri { builder ->
                builder.path("/api/alerts")
                    .queryParam("size", pageSize)
                    .queryParam("sortBy", "timestamp")
                    .queryParam("direction", "desc")
                    .build()
            }
            .retrieve()
            .bodyToMono(Map::class.java)
            .block()

    @Suppress("UNCHECKED_CAST")
    private fun extractContent(body: Map<*, *>?): List<Map<String, Any?>> {
        val data = body?.get("data") as? Map<*, *> ?: return emptyList()
        val content = data["content"] as? List<*> ?: return emptyList()
        return content.filterIsInstance<Map<*, *>>().map { it as Map<String, Any?> }
    }
}
