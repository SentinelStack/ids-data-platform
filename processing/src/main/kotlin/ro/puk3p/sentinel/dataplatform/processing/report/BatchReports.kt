package ro.puk3p.sentinel.dataplatform.processing.report

object BatchReports {
    private const val SEVERITY_RANK =
        "MAX(CASE severity WHEN 'CRITICAL' THEN 4 WHEN 'HIGH' THEN 3 " +
            "WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 1 ELSE 0 END)"

    fun all(view: String): List<Report> =
        listOf(
            Report(
                "top_source_ips",
                "SELECT sourceIp, COUNT(*) AS alert_count, " +
                    "COUNT(DISTINCT destinationIp) AS distinct_targets, " +
                    "COUNT(DISTINCT destinationPort) AS distinct_ports, " +
                    "$SEVERITY_RANK AS max_severity_rank " +
                    "FROM $view WHERE sourceIp IS NOT NULL " +
                    "GROUP BY sourceIp ORDER BY alert_count DESC LIMIT 100",
            ),
            Report(
                "alerts_per_device",
                "SELECT deviceId, COUNT(*) AS alert_count, " +
                    "COUNT(DISTINCT sourceIp) AS distinct_sources " +
                    "FROM $view GROUP BY deviceId ORDER BY alert_count DESC",
            ),
            Report(
                "severity_distribution",
                "SELECT COALESCE(severity, 'UNKNOWN') AS severity, COUNT(*) AS alert_count " +
                    "FROM $view GROUP BY COALESCE(severity, 'UNKNOWN') ORDER BY alert_count DESC",
            ),
            Report(
                "type_distribution",
                "SELECT COALESCE(type, 'UNKNOWN') AS type, COUNT(*) AS alert_count " +
                    "FROM $view GROUP BY COALESCE(type, 'UNKNOWN') ORDER BY alert_count DESC",
            ),
            Report(
                "daily_trend",
                "SELECT dt, COUNT(*) AS alert_count, " +
                    "COUNT(DISTINCT sourceIp) AS distinct_sources, " +
                    "COUNT(DISTINCT deviceId) AS active_devices " +
                    "FROM $view GROUP BY dt ORDER BY dt",
            ),
            Report(
                "top_destination_ports",
                "SELECT destinationPort, COUNT(*) AS alert_count " +
                    "FROM $view WHERE destinationPort IS NOT NULL " +
                    "GROUP BY destinationPort ORDER BY alert_count DESC LIMIT 50",
            ),
        )
}
