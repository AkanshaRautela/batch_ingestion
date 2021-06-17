package com.bip.spark.gcp

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object MoveToSchema extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def writeToSchema(fileName : String , date : String ) = {

    logger.info("write df to schema ")

//    val viewDF = spark.read.format("bigquery")
//      .option("table" , "").load().cache()

//    val schemaDF = viewDF.withColumn("deleteFlag","true")
//      .withColumn("Brand_Id","LBG")
//
//    schemaDF.show()
//
//    schemaDF.write.mode("APPEND").format("bigquery").option("temporaryGcsBucket","batch_ingestion_bucket").option("table", "schema_param_table.param_prop_tb").save()













  }

}
