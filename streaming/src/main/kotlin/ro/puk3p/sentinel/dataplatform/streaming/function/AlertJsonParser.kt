package ro.puk3p.sentinel.dataplatform.streaming.function

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector
import ro.puk3p.sentinel.dataplatform.streaming.model.AlertEvent

class AlertJsonParser : RichFlatMapFunction<String, AlertEvent>() {
    @Transient
    private var mapper: ObjectMapper? = null

    override fun flatMap(value: String?, out: Collector<AlertEvent>) {
        if (value.isNullOrBlank()) {
            return
        }
        val json = mapper ?: ObjectMapper().also { mapper = it }
        val node =
            try {
                json.readTree(value)
            } catch (ex: Exception) {
                return
            }
        if (node == null || !node.isObject) {
            return
        }
        val sourceIp = node.path("sourceIp").asText("").trim()
        if (sourceIp.isBlank()) {
            return
        }
        out.collect(
            AlertEvent(
                sourceIp = sourceIp,
                deviceId = node.path("deviceId").asText("").trim(),
                type = node.path("type").asText("").trim(),
                severity = node.path("severity").asText("LOW").trim().ifBlank { "LOW" },
                timestamp = node.path("timestamp").asText("").trim(),
            ),
        )
    }
}
