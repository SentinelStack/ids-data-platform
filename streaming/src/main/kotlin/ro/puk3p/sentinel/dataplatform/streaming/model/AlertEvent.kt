package ro.puk3p.sentinel.dataplatform.streaming.model

import java.io.Serializable

class AlertEvent() : Serializable {
    var sourceIp: String = ""
    var deviceId: String = ""
    var type: String = ""
    var severity: String = "LOW"
    var timestamp: String = ""

    constructor(sourceIp: String, deviceId: String, type: String, severity: String, timestamp: String) : this() {
        this.sourceIp = sourceIp
        this.deviceId = deviceId
        this.type = type
        this.severity = severity
        this.timestamp = timestamp
    }

    companion object {
        private const val serialVersionUID: Long = 1L
    }
}
