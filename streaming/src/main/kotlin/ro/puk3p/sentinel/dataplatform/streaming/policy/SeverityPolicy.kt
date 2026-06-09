package ro.puk3p.sentinel.dataplatform.streaming.policy

object SeverityPolicy {
    fun escalate(severities: Set<String>): String {
        val normalized = severities.map { it.trim().uppercase() }.toSet()
        return when {
            "CRITICAL" in normalized || "HIGH" in normalized -> "CRITICAL"
            "MEDIUM" in normalized -> "HIGH"
            else -> "MEDIUM"
        }
    }
}
