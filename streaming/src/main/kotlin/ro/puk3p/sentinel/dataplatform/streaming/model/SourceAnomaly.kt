package ro.puk3p.sentinel.dataplatform.streaming.model

data class SourceAnomaly(
    val sourceIp: String,
    val deviceId: String,
    val deviceIds: List<String>,
    val alertCount: Int,
    val severity: String,
    val windowStart: String,
    val windowEnd: String,
    val alertTypes: List<String>,
    val type: String = "ESCALATED_SOURCE",
)
