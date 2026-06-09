package ro.puk3p.sentinel.dataplatform.streaming

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import java.io.Serializable
import java.time.Duration
import java.time.Instant

/**
 * Flink real-time anomaly detector.
 *
 * Consumes IDS alert events from Kafka (`ids.alerts`), keys them by source IP,
 * and over a tumbling processing-time window escalates a source that trips
 * >= ALERT_THRESHOLD alerts in the window into a correlated anomaly published
 * to `traffic-anomalies`.
 *
 * Config via env: KAFKA_BOOTSTRAP, ALERTS_TOPIC, ANOMALIES_TOPIC,
 * WINDOW_SECONDS, ALERT_THRESHOLD.
 */
fun main() {
    val bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092")
    val inTopic = env("ALERTS_TOPIC", "ids.alerts")
    val outTopic = env("ANOMALIES_TOPIC", "traffic-anomalies")
    val windowSeconds = env("WINDOW_SECONDS", "60").toLong()
    val threshold = env("ALERT_THRESHOLD", "3").toInt()

    val see = StreamExecutionEnvironment.getExecutionEnvironment()

    val source = KafkaSource.builder<String>()
        .setBootstrapServers(bootstrap)
        .setTopics(inTopic)
        .setGroupId("flink-anomaly-detector")
        .setStartingOffsets(OffsetsInitializer.earliest())
        .setValueOnlyDeserializer(SimpleStringSchema())
        .build()

    val sink = KafkaSink.builder<String>()
        .setBootstrapServers(bootstrap)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder<String>()
                .setTopic(outTopic)
                .setValueSerializationSchema(SimpleStringSchema())
                .build(),
        )
        .build()

    see.fromSource(source, WatermarkStrategy.noWatermarks(), "ids.alerts")
        .flatMap(ParseAlert())
        .keyBy(SourceIpKey())
        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(windowSeconds)))
        .apply(AnomalyWindow(threshold))
        .sinkTo(sink)

    see.execute("sentinel-flink-anomaly-detector")
}

private fun env(key: String, default: String): String = System.getenv(key) ?: default

/** Flink POJO for a parsed alert (no-arg ctor + mutable fields). */
class AlertEvent() : Serializable {
    var sourceIp: String = ""
    var deviceId: String = ""
    var type: String = ""
    var severity: String = ""
    var timestamp: String = ""

    constructor(sourceIp: String, deviceId: String, type: String, severity: String, timestamp: String) : this() {
        this.sourceIp = sourceIp
        this.deviceId = deviceId
        this.type = type
        this.severity = severity
        this.timestamp = timestamp
    }
}

/** Parses an alert JSON string into an AlertEvent; skips records without a source IP. */
class ParseAlert : RichFlatMapFunction<String, AlertEvent>() {
    @Transient
    private var mapper: ObjectMapper? = null

    override fun flatMap(value: String, out: Collector<AlertEvent>) {
        val m = mapper ?: ObjectMapper().also { mapper = it }
        try {
            val node = m.readTree(value)
            val sourceIp = node.path("sourceIp").asText("")
            if (sourceIp.isBlank()) {
                return
            }
            out.collect(
                AlertEvent(
                    sourceIp = sourceIp,
                    deviceId = node.path("deviceId").asText(""),
                    type = node.path("type").asText(""),
                    severity = node.path("severity").asText("LOW"),
                    timestamp = node.path("timestamp").asText(""),
                ),
            )
        } catch (ex: Exception) {
            // ignore malformed events
        }
    }
}

class SourceIpKey : KeySelector<AlertEvent, String> {
    override fun getKey(value: AlertEvent): String = value.sourceIp
}

/** Escalates a source IP that trips >= threshold alerts within a window. */
class AnomalyWindow(private val threshold: Int) : WindowFunction<AlertEvent, String, String, TimeWindow> {
    override fun apply(key: String, window: TimeWindow, input: Iterable<AlertEvent>, out: Collector<String>) {
        var count = 0
        var deviceId = ""
        val severities = HashSet<String>()
        val types = HashSet<String>()
        for (e in input) {
            count++
            deviceId = e.deviceId
            severities.add(e.severity)
            types.add(e.type)
        }
        if (count < threshold) {
            return
        }

        val severity = escalate(severities)
        val typesJson = types.joinToString(prefix = "[", postfix = "]") { "\"$it\"" }
        val json =
            """{"type":"ESCALATED_SOURCE","sourceIp":"$key","deviceId":"$deviceId",""" +
                """"alertCount":$count,"severity":"$severity",""" +
                """"windowStart":"${Instant.ofEpochMilli(window.start)}",""" +
                """"windowEnd":"${Instant.ofEpochMilli(window.end)}","alertTypes":$typesJson}"""
        out.collect(json)
    }

    private fun escalate(severities: Set<String>): String =
        when {
            "CRITICAL" in severities || "HIGH" in severities -> "CRITICAL"
            "MEDIUM" in severities -> "HIGH"
            else -> "MEDIUM"
        }
}
