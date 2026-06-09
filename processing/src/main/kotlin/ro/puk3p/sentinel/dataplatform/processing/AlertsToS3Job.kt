package ro.puk3p.sentinel.dataplatform.processing

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.to_timestamp
import ro.puk3p.sentinel.dataplatform.processing.support.AlertSchema
import ro.puk3p.sentinel.dataplatform.processing.support.Env
import ro.puk3p.sentinel.dataplatform.processing.support.SparkSessionFactory

fun main() {
    val kafkaBootstrap = Env.get("KAFKA_BOOTSTRAP", "localhost:9092")
    val topic = Env.get("ALERTS_TOPIC", "ids.alerts")
    val s3Output = Env.get("S3_OUTPUT", "s3a://sentinel-ids-lake/alerts")
    val s3Checkpoint = Env.get("S3_CHECKPOINT", "s3a://sentinel-ids-lake/_checkpoints/alerts")

    val spark = SparkSessionFactory.create("sentinel-alerts-to-s3")

    val kafka =
        spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()

    val alerts =
        kafka
            .selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts")
            .select(from_json(col("json"), AlertSchema.struct()).alias("a"), col("kafka_ts"))
            .select("a.*", "kafka_ts")
            .filter(col("alertId").isNotNull)
            .withColumn("event_ts", to_timestamp(col("timestamp")))
            .withColumn("dt", to_date(col("event_ts")))

    val query =
        alerts.writeStream()
            .format("parquet")
            .option("path", s3Output)
            .option("checkpointLocation", s3Checkpoint)
            .partitionBy("dt")
            .outputMode("append")
            .start()

    query.awaitTermination()
}
