package ro.puk3p.sentinel.dataplatform.processing

import org.apache.spark.sql.SparkSession

/**
 * Spark batch analytics job: reads the historical alert lake (Parquet on S3
 * written by AlertsToS3Job) and computes retrospective aggregations — top
 * talkers, per-device volume, severity/type distributions, daily trends and
 * top destination ports — writing each as a curated Parquet report back to S3.
 *
 * This is the historical/batch counterpart to the Flink real-time job: it runs
 * to completion and exits (scheduled via a systemd timer), rather than
 * streaming continuously.
 *
 * Run: java -cp app.jar ro.puk3p.sentinel.dataplatform.processing.BatchAnalyticsJobKt
 *
 * Config via env: S3_INPUT, S3_REPORTS, SPARK_MASTER, AWS_REGION,
 * AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
 */
fun main() {
    val input = env("S3_INPUT", "s3a://sentinel-ids-lake/alerts")
    val reports = env("S3_REPORTS", "s3a://sentinel-ids-lake/reports")
    val master = env("SPARK_MASTER", "local[*]")

    val spark = SparkSession.builder()
        .appName("sentinel-batch-analytics")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", env("AWS_REGION", "eu-north-1"))
        .getOrCreate()

    try {
        val df = spark.read().parquet(input)
        df.createOrReplaceTempView("alerts")
        val total = df.count()
        println("[batch-analytics] loaded $total alert rows from $input")

        val severityRank =
            "MAX(CASE severity WHEN 'CRITICAL' THEN 4 WHEN 'HIGH' THEN 3 " +
                "WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 1 ELSE 0 END)"

        writeReport(
            spark, "$reports/top_source_ips",
            "SELECT sourceIp, COUNT(*) AS alert_count, " +
                "COUNT(DISTINCT destinationIp) AS distinct_targets, " +
                "COUNT(DISTINCT destinationPort) AS distinct_ports, " +
                "$severityRank AS max_severity_rank " +
                "FROM alerts WHERE sourceIp IS NOT NULL " +
                "GROUP BY sourceIp ORDER BY alert_count DESC LIMIT 100",
        )

        writeReport(
            spark, "$reports/alerts_per_device",
            "SELECT deviceId, COUNT(*) AS alert_count, " +
                "COUNT(DISTINCT sourceIp) AS distinct_sources " +
                "FROM alerts GROUP BY deviceId ORDER BY alert_count DESC",
        )

        writeReport(
            spark, "$reports/severity_distribution",
            "SELECT severity, COUNT(*) AS alert_count " +
                "FROM alerts GROUP BY severity ORDER BY alert_count DESC",
        )

        writeReport(
            spark, "$reports/type_distribution",
            "SELECT type, COUNT(*) AS alert_count " +
                "FROM alerts GROUP BY type ORDER BY alert_count DESC",
        )

        writeReport(
            spark, "$reports/daily_trend",
            "SELECT dt, COUNT(*) AS alert_count, " +
                "COUNT(DISTINCT sourceIp) AS distinct_sources, " +
                "COUNT(DISTINCT deviceId) AS active_devices " +
                "FROM alerts GROUP BY dt ORDER BY dt",
        )

        writeReport(
            spark, "$reports/top_destination_ports",
            "SELECT destinationPort, COUNT(*) AS alert_count " +
                "FROM alerts WHERE destinationPort IS NOT NULL " +
                "GROUP BY destinationPort ORDER BY alert_count DESC LIMIT 50",
        )

        println("[batch-analytics] done — reports under $reports")
    } finally {
        spark.stop()
    }
}

private fun writeReport(spark: SparkSession, path: String, sql: String) {
    val result = spark.sql(sql)
    result.coalesce(1).write().mode("overwrite").parquet(path)
    println("[batch-analytics] wrote $path")
}

private fun env(key: String, default: String): String = System.getenv(key) ?: default
