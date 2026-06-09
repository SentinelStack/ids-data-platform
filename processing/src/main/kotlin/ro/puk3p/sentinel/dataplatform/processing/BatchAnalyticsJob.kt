package ro.puk3p.sentinel.dataplatform.processing

import org.apache.spark.sql.SparkSession
import ro.puk3p.sentinel.dataplatform.processing.report.BatchReports
import ro.puk3p.sentinel.dataplatform.processing.support.Env
import ro.puk3p.sentinel.dataplatform.processing.support.SparkSessionFactory

fun main() {
    val input = Env.get("S3_INPUT", "s3a://sentinel-ids-lake/alerts")
    val reports = Env.get("S3_REPORTS", "s3a://sentinel-ids-lake/reports")

    val spark = SparkSessionFactory.create("sentinel-batch-analytics")
    try {
        val df =
            try {
                spark.read().parquet(input)
            } catch (ex: Exception) {
                println("[batch-analytics] no readable input at $input: ${ex.message}")
                return
            }

        val view = "alerts"
        df.createOrReplaceTempView(view)
        val total = df.count()
        println("[batch-analytics] loaded $total alert rows from $input")
        if (total == 0L) {
            println("[batch-analytics] nothing to analyze; skipping report generation")
            return
        }

        for (report in BatchReports.all(view)) {
            writeReport(spark, "$reports/${report.name}", report.sql)
        }
        println("[batch-analytics] done — reports under $reports")
    } finally {
        spark.stop()
    }
}

private fun writeReport(spark: SparkSession, path: String, sql: String) {
    try {
        spark.sql(sql).coalesce(1).write().mode("overwrite").parquet(path)
        println("[batch-analytics] wrote $path")
    } catch (ex: Exception) {
        println("[batch-analytics] report $path failed: ${ex.message}")
    }
}
