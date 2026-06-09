package ro.puk3p.sentinel.dataplatform.streaming

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.sink.KafkaSink
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import ro.puk3p.sentinel.dataplatform.streaming.config.JobConfig
import ro.puk3p.sentinel.dataplatform.streaming.function.AlertJsonParser
import ro.puk3p.sentinel.dataplatform.streaming.function.SourceAnomalyWindow
import ro.puk3p.sentinel.dataplatform.streaming.function.SourceIpKeySelector
import java.time.Duration

fun main() {
    val config = JobConfig.fromEnv()
    val env = StreamExecutionEnvironment.getExecutionEnvironment()

    val source =
        KafkaSource.builder<String>()
            .setBootstrapServers(config.bootstrapServers)
            .setTopics(config.alertsTopic)
            .setGroupId(config.groupId)
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(SimpleStringSchema())
            .build()

    val sink =
        KafkaSink.builder<String>()
            .setBootstrapServers(config.bootstrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder<String>()
                    .setTopic(config.anomaliesTopic)
                    .setValueSerializationSchema(SimpleStringSchema())
                    .build(),
            )
            .build()

    env.fromSource(source, WatermarkStrategy.noWatermarks(), "ids.alerts")
        .flatMap(AlertJsonParser())
        .keyBy(SourceIpKeySelector())
        .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(config.windowSeconds)))
        .apply(SourceAnomalyWindow(config.alertThreshold))
        .sinkTo(sink)

    env.execute("sentinel-flink-anomaly-detector")
}
