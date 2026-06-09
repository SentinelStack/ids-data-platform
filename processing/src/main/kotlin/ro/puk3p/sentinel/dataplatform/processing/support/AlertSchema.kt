package ro.puk3p.sentinel.dataplatform.processing.support

import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType

object AlertSchema {
    fun struct(): StructType =
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
}
