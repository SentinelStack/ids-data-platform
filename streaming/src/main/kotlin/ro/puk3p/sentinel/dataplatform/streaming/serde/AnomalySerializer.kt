package ro.puk3p.sentinel.dataplatform.streaming.serde

import ro.puk3p.sentinel.dataplatform.streaming.model.SourceAnomaly

object AnomalySerializer {
    fun toJson(anomaly: SourceAnomaly): String {
        val sb = StringBuilder(160)
        sb.append('{')
        appendString(sb, "type", anomaly.type).append(',')
        appendString(sb, "sourceIp", anomaly.sourceIp).append(',')
        appendString(sb, "deviceId", anomaly.deviceId).append(',')
        sb.append("\"deviceIds\":[")
        anomaly.deviceIds.forEachIndexed { index, value ->
            if (index > 0) {
                sb.append(',')
            }
            sb.append('"').append(escape(value)).append('"')
        }
        sb.append("],")
        sb.append("\"alertCount\":").append(anomaly.alertCount).append(',')
        appendString(sb, "severity", anomaly.severity).append(',')
        appendString(sb, "windowStart", anomaly.windowStart).append(',')
        appendString(sb, "windowEnd", anomaly.windowEnd).append(',')
        sb.append("\"alertTypes\":[")
        anomaly.alertTypes.forEachIndexed { index, value ->
            if (index > 0) {
                sb.append(',')
            }
            sb.append('"').append(escape(value)).append('"')
        }
        sb.append("]}")
        return sb.toString()
    }

    private fun appendString(sb: StringBuilder, name: String, value: String): StringBuilder =
        sb.append('"').append(name).append("\":\"").append(escape(value)).append('"')

    private fun escape(value: String): String {
        val sb = StringBuilder(value.length + 8)
        for (c in value) {
            when (c) {
                '\\' -> sb.append("\\\\")
                '"' -> sb.append("\\\"")
                '\n' -> sb.append("\\n")
                '\r' -> sb.append("\\r")
                '\t' -> sb.append("\\t")
                else -> if (c < ' ') sb.append("\\u").append(c.code.toString(16).padStart(4, '0')) else sb.append(c)
            }
        }
        return sb.toString()
    }
}
