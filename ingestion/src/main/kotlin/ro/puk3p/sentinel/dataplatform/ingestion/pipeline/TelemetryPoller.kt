package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import ro.puk3p.sentinel.dataplatform.ingestion.client.BackendClient
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties
import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

@Component
class TelemetryPoller(
    private val backend: BackendClient,
    private val kafka: KafkaTemplate<String, String>,
    private val props: IngestionProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val alertWatermark = AtomicReference<Instant?>(null)

    @Scheduled(fixedDelayString = "\${ingestion.poll-interval-ms:5000}", initialDelay = 3000)
    fun pollAlerts() {
        val from = alertWatermark.get()
        val body = backend.fetchAlertsSince(from) ?: return
        val content = body.path("data").path("content")
        if (!content.isArray || content.isEmpty) {
            return
        }

        var maxTs = from
        var count = 0
        for (node in content) {
            val deviceId = node.path("deviceId").asText("unknown")
            kafka.send(props.topics.alerts, deviceId, node.toString())
            count++
            val ts = parseInstant(node.path("timestamp").asText(null))
            if (ts != null && (maxTs == null || ts.isAfter(maxTs))) {
                maxTs = ts
            }
        }

        // Advance just past the newest event so the inclusive 'from' filter does not re-fetch it.
        if (maxTs != null) {
            alertWatermark.set(maxTs.plusMillis(1))
        }
        log.info("ingested {} alert(s) -> topic {}", count, props.topics.alerts)
    }

    private fun parseInstant(value: String?): Instant? =
        try {
            if (value.isNullOrBlank()) null else Instant.parse(value)
        } catch (ex: Exception) {
            null
        }
}
