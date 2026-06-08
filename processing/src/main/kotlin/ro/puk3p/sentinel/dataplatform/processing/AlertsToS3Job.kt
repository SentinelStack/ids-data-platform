package ro.puk3p.sentinel.dataplatform.processing

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

/**
 * Spark Structured Streaming job: consumes IDS alert events from Kafka, parses
 * the JSON, and appends them as date-partitioned Parquet to the S3 data lake.
 *
 * Run (local): java -jar ids-data-platform-processing.jar
 * Or submit:   spark-submit --class ...AlertsToS3JobKt ids-data-platform-processing.jar
 *
 * Config via env: KAFKA_BOOTSTRAP, ALERTS_TOPIC, S3_OUTPUT, S3_CHECKPOINT,
 * SPARK_MASTER, AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
 */
fun main() {
    val kafkaBootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092")
    val topic = env("ALERTS_TOPIC", "ids.alerts")
    val s3Output = env("S3_OUTPUT", "s3a://sentinel-ids-lake/alerts")
    val s3Checkpoint = env("S3_CHECKPOINT", "s3a://sentinel-ids-lake/_checkpoints/alerts")
    val master = env("SPARK_MASTER", "local[*]")

    val spark = SparkSession.builder()
        .appName("sentinel-alerts-to-s3")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", env("AWS_REGION", "eu-central-1"))
        .getOrCreate()

    val schema = alertSchema()

    val kafka = spark.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaBootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

    val alerts = kafka
        .selectExpr("CAST(value AS STRING) AS json", "timestamp AS kafka_ts")
        .select(from_json(col("json"), schema).alias("a"), col("kafka_ts"))
        .select("a.*", "kafka_ts")
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("dt", to_date(col("event_ts")))

    val query = alerts.writeStream()
        .format("parquet")
        .option("path", s3Output)
        .option("checkpointLocation", s3Checkpoint)
        .partitionBy("dt")
        .outputMode("append")
        .start()

    query.awaitTermination()
}

private fun alertSchema(): StructType =
    StructType()
        .add("alertId", DataTypes.StringType)
        .add("deviceId", DataTypes.StringType)
        .add("timestamp", DataTypes.StringType)
        .add("type", DataTypes.StringType)
        .add("severity", DataTypes.StringType)
        .add("protocol", DataTypes.StringType)
        .add("sourceIp", DataTypes.StringType)
        .add("destinationIp", DataTypes.StringType)
        .add("sourcePort", DataTypes.IntegerType)
        .add("destinationPort", DataTypes.IntegerType)
        .add("packetCount", DataTypes.LongType)
        .add("bytesCount", DataTypes.LongType)
        .add("windowSeconds", DataTypes.IntegerType)
        .add("description", DataTypes.StringType)
        .add("acknowledged", DataTypes.BooleanType)
        .add("createdAt", DataTypes.StringType)

private fun env(key: String, default: String): String = System.getenv(key) ?: default
