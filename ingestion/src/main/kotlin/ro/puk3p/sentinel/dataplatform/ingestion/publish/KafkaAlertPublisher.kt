package ro.puk3p.sentinel.dataplatform.ingestion.publish

import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import ro.puk3p.sentinel.dataplatform.ingestion.config.IngestionProperties
import tools.jackson.databind.ObjectMapper
import java.util.concurrent.TimeUnit

@Component
class KafkaAlertPublisher(
    private val kafka: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    props: IngestionProperties,
) : AlertPublisher {
    private val log = LoggerFactory.getLogger(javaClass)
    private val topic = props.topics.alerts
    private val sendTimeoutSeconds = props.sendTimeoutSeconds

    override fun publish(key: String, alert: Map<String, Any?>): Boolean =
        try {
            kafka.send(topic, key, objectMapper.writeValueAsString(alert)).get(sendTimeoutSeconds, TimeUnit.SECONDS)
            true
        } catch (ex: Exception) {
            log.warn("publish to {} failed for key {}: {}", topic, key, ex.message)
            false
        }
}
