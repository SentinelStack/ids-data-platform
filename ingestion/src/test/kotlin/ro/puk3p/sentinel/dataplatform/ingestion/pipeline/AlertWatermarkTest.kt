package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

class AlertWatermarkTest {
    private val t1 = Instant.parse("2026-06-11T10:00:00Z")
    private val t2 = Instant.parse("2026-06-11T10:00:01Z")

    @Test
    fun `newest-first batch is fully accepted against the batch-start snapshot`() {
        val watermark = AlertWatermark()
        watermark.observe(t1, "old")
        val since = watermark.current()

        assertTrue(watermark.isNew(since, t2, "c"))
        watermark.observe(t2, "c")
        assertTrue(watermark.isNew(since, t2, "b"))
        watermark.observe(t2, "b")
        assertTrue(watermark.isNew(since, t2, "a"))
    }

    @Test
    fun `same-timestamp alert with a new id is accepted on the next poll`() {
        val watermark = AlertWatermark()
        watermark.observe(t2, "a")
        val since = watermark.current()

        assertTrue(watermark.isNew(since, t2, "b"))
        assertFalse(watermark.isNew(since, t2, "a"))
    }

    @Test
    fun `older alerts are rejected`() {
        val watermark = AlertWatermark()
        watermark.observe(t2, "a")
        assertFalse(watermark.isNew(watermark.current(), t1, "x"))
    }

    @Test
    fun `null timestamp or empty watermark is accepted`() {
        val watermark = AlertWatermark()
        assertTrue(watermark.isNew(watermark.current(), t1, "x"))
        watermark.observe(t1, "x")
        assertTrue(watermark.isNew(watermark.current(), null, "y"))
    }
}
