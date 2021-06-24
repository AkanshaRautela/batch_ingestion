package com.bip.spark.gcp

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes

object MoveToSchema extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def writeToSchema(rawDF : DataFrame, fileName : String , date : String ) = {

    logger.info("write df to bq schema table")

    val df = spark.read.format("bigquery")
                       .option("table", "batch_ingestion_schema.customer_record_table")
      .load().cache()

    val newDF = df.drop("BrandId").drop("DeleteFlag").drop("FileName")
      .drop("_PARTITIONTIME").drop("_PARTITIONDATE")

    val cols : Array[String] = newDF.columns

    logger.info(" Table schema " + cols.mkString)

    val renamedColsDF = rawDF.toDF(cols:_*)

    renamedColsDF.printSchema()

    val schemaDF = renamedColsDF.withColumn("BrandId",lit("LBG").cast(DataTypes.StringType))
      .withColumn("DeleteFlag",lit("false").cast(DataTypes.BooleanType))
      .withColumn("FileName", lit(fileName).cast(DataTypes.StringType))

    schemaDF.show()

    schemaDF.write.mode("APPEND").format("bigquery").partitionBy(date)
      .option("temporaryGcsBucket","batch_ingestion_bucket")
      .option("table", "batch_ingestion_schema.customer_record_table")
      .save()













  }

}
