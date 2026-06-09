package ro.puk3p.sentinel.dataplatform.processing.support

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {
    fun create(appName: String): SparkSession =
        SparkSession.builder()
            .appName(appName)
            .master(Env.get("SPARK_MASTER", "local[*]"))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint.region", Env.get("AWS_REGION", "eu-north-1"))
            .getOrCreate()
}
