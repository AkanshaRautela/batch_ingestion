package com.ingestion.spark.gcp

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.DataTypes

import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object MoveToSchema extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def writeToSchema(rawDF : DataFrame, fileName : String , date : String , tableName : String , dataset : String ) = {

    logger.info("write df to bq schema table")

    //val time : String = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(date)

    val runTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new SimpleDateFormat("yyyyMMdd").parse(date))

//    val df = spark.read.format("bigquery")
//                        .option("table", dataset + "." + tableName)
//                        .option("filter", "Ingestion_Date < CURRENT_TIMESTAMP()")
//                        .option("query", "select * from  " + dataset + "." + tableName +  "  limit 1")
//                        .load().cache()
//
//    df.show()
//
//    val newDF = df.drop("BrandId").drop("DeleteFlag").drop("FileName")
//      .drop("Ingestion_Date")
//
//    val cols : Array[String] = newDF.columns
//
//    logger.info(" Table schema " + cols.mkString)
//
//    var renamedColsDF = rawDF.toDF(cols:_*)
//
//    renamedColsDF.printSchema()
//
////    renamedColsDF = spark.createDataFrame(renamedColsDF.rdd , newDF.schema)
////
////    renamedColsDF.printSchema()

    val schemaDF = rawDF.withColumn("BrandId",lit("EXT").cast(DataTypes.StringType))
      .withColumn("DeleteFlag",lit("false").cast(DataTypes.StringType))
      .withColumn("FileName", lit(fileName).cast(DataTypes.StringType))
      .withColumn("Ingestion_Date", lit(runTime).cast(DataTypes.TimestampType))

    schemaDF.show()

    schemaDF.write.mode("APPEND").format("bigquery")
      .option("temporaryGcsBucket","batch_ingestion_bucket")
      .option("table", dataset + "." + tableName)
      .save()













  }

}
