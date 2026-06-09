package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

class AlertWatermark {
    private val seen = AtomicReference<Instant?>(null)

    fun isNew(timestamp: Instant?): Boolean {
        if (timestamp == null) {
            return true
        }
        val current = seen.get()
        return current == null || timestamp.isAfter(current)
    }

    fun observe(timestamp: Instant?) {
        if (timestamp == null) {
            return
        }
        seen.updateAndGet { current ->
            if (current == null || timestamp.isAfter(current)) timestamp else current
        }
    }
}
