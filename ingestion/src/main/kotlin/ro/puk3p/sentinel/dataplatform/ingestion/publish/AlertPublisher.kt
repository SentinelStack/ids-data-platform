package ro.puk3p.sentinel.dataplatform.ingestion.publish

interface AlertPublisher {
    fun publish(key: String, alert: Map<String, Any?>): Boolean
}
