package ro.puk3p.sentinel.dataplatform.ingestion.client

interface AlertSource {
    fun fetchLatestAlerts(): List<Map<String, Any?>>
}
