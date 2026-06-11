package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class AlertWatermark(private val store: WatermarkStore? = null) {
    data class Mark(val timestamp: Instant, val idsAtTimestamp: Set<String>)

    private val state = AtomicReference<Mark?>(store?.load()?.let { Mark(it, emptySet()) })

    fun current(): Mark? = state.get()

    fun isNew(against: Mark?, timestamp: Instant?, alertId: String?): Boolean {
        if (timestamp == null || against == null) {
            return true
        }
        if (timestamp.isAfter(against.timestamp)) {
            return true
        }
        return timestamp == against.timestamp && alertId != null && alertId !in against.idsAtTimestamp
    }

    fun observe(timestamp: Instant?, alertId: String?) {
        if (timestamp == null) {
            return
        }
        val updated =
            state.updateAndGet { mark ->
                when {
                    mark == null || timestamp.isAfter(mark.timestamp) ->
                        Mark(timestamp, alertId?.let { setOf(it) } ?: emptySet())
                    timestamp == mark.timestamp && alertId != null ->
                        Mark(mark.timestamp, mark.idsAtTimestamp + alertId)
                    else -> mark
                }
            }
        if (updated != null && updated.timestamp == timestamp) {
            store?.save(updated.timestamp)
        }
    }
}
