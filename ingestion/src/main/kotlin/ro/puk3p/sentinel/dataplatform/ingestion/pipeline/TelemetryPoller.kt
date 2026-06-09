package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import ro.puk3p.sentinel.dataplatform.ingestion.client.BackendClient
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties
import tools.jackson.databind.ObjectMapper
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

@Component
class TelemetryPoller(
    private val backend: BackendClient,
    private val kafka: KafkaTemplate<String, String>,
    private val props: IngestionProperties,
    private val objectMapper: ObjectMapper,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val alertWatermark = AtomicReference<Instant?>(null)

    @Scheduled(fixedDelayString = "\${ingestion.poll-interval-ms:5000}", initialDelay = 3000)
    fun pollAlerts() {
        val resp =
            try {
                backend.fetchLatestAlerts()
            } catch (ex: Exception) {
                log.warn("alert poll failed: {}", ex.message)
                null
            } ?: return

        val data = resp["data"] as? Map<*, *> ?: return
        val content = data["content"] as? List<*> ?: return
        if (content.isEmpty()) {
            return
        }

        val since = alertWatermark.get()
        var maxTs = since
        var count = 0
        for (item in content) {
            val alert = item as? Map<*, *> ?: continue
            val ts = (alert["timestamp"] as? String)?.let { parseInstant(it) }
            // Skip events we've already emitted (client-side incremental watermark).
            if (since != null && ts != null && !ts.isAfter(since)) {
                continue
            }
            val deviceId = alert["deviceId"]?.toString() ?: "unknown"
            kafka.send(props.topics.alerts, deviceId, objectMapper.writeValueAsString(alert))
            count++
            if (ts != null && (maxTs == null || ts.isAfter(maxTs))) {
                maxTs = ts
            }
        }

        if (maxTs != null) {
            alertWatermark.set(maxTs)
        }
        if (count > 0) {
            log.info("ingested {} alert(s) -> topic {}", count, props.topics.alerts)
        }
    }

    private fun parseInstant(value: String?): Instant? =
        try {
            if (value.isNullOrBlank()) null else Instant.parse(value)
        } catch (ex: Exception) {
            null
        }
}
