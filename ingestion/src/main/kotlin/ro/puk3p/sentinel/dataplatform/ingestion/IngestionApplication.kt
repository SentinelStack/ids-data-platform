package ro.puk3p.sentinel.dataplatform.ingestion

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
@ConfigurationPropertiesScan
class IngestionApplication

fun main(args: Array<String>) {
    runApplication<IngestionApplication>(*args)
}
