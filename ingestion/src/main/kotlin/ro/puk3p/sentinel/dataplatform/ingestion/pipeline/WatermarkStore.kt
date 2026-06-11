package ro.puk3p.sentinel.dataplatform.ingestion.pipeline

import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant

interface WatermarkStore {
    fun load(): Instant?

    fun save(value: Instant)
}

class FileWatermarkStore(private val path: Path) : WatermarkStore {
    private val log = LoggerFactory.getLogger(javaClass)

    override fun load(): Instant? {
        if (!Files.exists(path)) {
            return null
        }
        return try {
            Instant.parse(Files.readString(path).trim())
        } catch (ex: Exception) {
            log.warn("could not read watermark from {}: {}", path, ex.message)
            null
        }
    }

    override fun save(value: Instant) {
        try {
            path.parent?.let { Files.createDirectories(it) }
            val tmp = path.resolveSibling("${path.fileName}.tmp")
            Files.writeString(tmp, value.toString())
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        } catch (ex: Exception) {
            log.warn("could not persist watermark to {}: {}", path, ex.message)
        }
    }
}
