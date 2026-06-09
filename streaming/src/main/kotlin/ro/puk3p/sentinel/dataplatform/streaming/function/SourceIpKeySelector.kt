package ro.puk3p.sentinel.dataplatform.streaming.function

import org.apache.flink.api.java.functions.KeySelector
import ro.puk3p.sentinel.dataplatform.streaming.model.AlertEvent

class SourceIpKeySelector : KeySelector<AlertEvent, String> {
    override fun getKey(value: AlertEvent): String = value.sourceIp
}
