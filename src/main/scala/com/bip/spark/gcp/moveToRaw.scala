package com.bip.spark.gcp

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object moveToRaw extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def writeToRaw(df:DataFrame,rawPath : String, opcadate : String) = {

    logger.info("Writing the df into raw google cloud storage " + rawPath)

    df.repartition(1).write
      .mode("APPEND")
      .option("delimiter", ",")
      .option("quote","")
      .format("com.databricks.spark.csv")
      .csv(rawPath)



  }

}
