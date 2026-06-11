package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import ro.puk3p.sentinel.dataplatform.ingestion.client.AlertSource
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties
import ro.puk3p.sentinel.dataplatform.ingestion.publish.AlertPublisher
import java.nio.file.Path
import java.time.Instant

@Component
class TelemetryPoller(
    private val source: AlertSource,
    private val publisher: AlertPublisher,
    props: IngestionProperties,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val watermark = AlertWatermark(FileWatermarkStore(Path.of(props.watermarkFile)))

    @Scheduled(fixedDelayString = "\${ingestion.poll-interval-ms:5000}", initialDelay = 3000)
    fun pollAlerts() {
        val alerts = source.fetchLatestAlerts()
        if (alerts.isEmpty()) {
            return
        }

        var published = 0
        for (alert in alerts) {
            val timestamp = parseInstant(alert["timestamp"] as? String)
            if (!watermark.isNew(timestamp)) {
                continue
            }
            if (publisher.publish(keyOf(alert), alert)) {
                published++
                watermark.observe(timestamp)
            }
        }

        if (published > 0) {
            log.info("ingested {} alert(s)", published)
        }
    }

    private fun keyOf(alert: Map<String, Any?>): String =
        alert["deviceId"]?.toString()?.takeIf { it.isNotBlank() } ?: "unknown"

    private fun parseInstant(value: String?): Instant? {
        if (value.isNullOrBlank()) {
            return null
        }
        return try {
            Instant.parse(value)
        } catch (ex: Exception) {
            null
        }
    }
}
