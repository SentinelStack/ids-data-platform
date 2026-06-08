package ro.puk3p.sentinel.dataplatform.ingestion.config

import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder

@Configuration
class KafkaTopicsConfig(
    private val props: IngestionProperties,
) {
    @Bean
    fun alertsTopic(): NewTopic = TopicBuilder.name(props.topics.alerts).partitions(3).replicas(1).build()

    @Bean
    fun trafficTopic(): NewTopic = TopicBuilder.name(props.topics.traffic).partitions(3).replicas(1).build()
}
