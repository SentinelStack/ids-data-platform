package ro.puk3p.sentinel.dataplatform.streaming.function

import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import ro.puk3p.sentinel.dataplatform.streaming.model.AlertEvent
import ro.puk3p.sentinel.dataplatform.streaming.model.SourceAnomaly
import ro.puk3p.sentinel.dataplatform.streaming.policy.SeverityPolicy
import ro.puk3p.sentinel.dataplatform.streaming.serde.AnomalySerializer
import java.time.Instant

class SourceAnomalyWindow(private val threshold: Int) : WindowFunction<AlertEvent, String, String, TimeWindow> {
    override fun apply(key: String, window: TimeWindow, input: Iterable<AlertEvent>, out: Collector<String>) {
        var count = 0
        val deviceIds = LinkedHashSet<String>()
        val severities = HashSet<String>()
        val types = LinkedHashSet<String>()
        for (event in input) {
            count++
            if (event.deviceId.isNotBlank()) {
                deviceIds.add(event.deviceId)
            }
            if (event.severity.isNotBlank()) {
                severities.add(event.severity)
            }
            if (event.type.isNotBlank()) {
                types.add(event.type)
            }
        }
        if (count < threshold) {
            return
        }

        val anomaly =
            SourceAnomaly(
                sourceIp = key,
                deviceId = deviceIds.firstOrNull() ?: "",
                deviceIds = deviceIds.toList(),
                alertCount = count,
                severity = SeverityPolicy.escalate(severities),
                windowStart = Instant.ofEpochMilli(window.start).toString(),
                windowEnd = Instant.ofEpochMilli(window.end).toString(),
                alertTypes = types.toList(),
            )
        out.collect(AnomalySerializer.toJson(anomaly))
    }
}
