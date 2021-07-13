package com.ingestion.spark.gcp

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path,FileSystem}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import com.google.cloud.spark.bigquery.SchemaConverters
import com.google.cloud.storage.Blob
import com.google.cloud.storage.CopyWriter
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions

import com.google.api.gax.paging.Page


import java.text.SimpleDateFormat

object moveToRaw extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def writeToRaw(df:DataFrame,rawPath : String, opcadate : String, fileName : String, table : String, dsName : String ) = {

    logger.info("Writing the df into raw google cloud storage " + rawPath)

    val sdf = new SimpleDateFormat("yyyyMMdd")

    var year = opcadate.substring(0,4).toInt
    var month = opcadate.substring(4,6).toInt
    var day = opcadate.substring(6,8).toInt

    val rawLocation = "GOOD" + "/year=" + year + "/" + "month=" + month + "/" + "day=" + day + "/"

    logger.info("Raw location required is " + rawLocation)

    val newRawLocation = rawPath + rawLocation

    logger.info("New Raw location required is " + newRawLocation)

    df.repartition(1).write
      .mode("APPEND")
      .option("delimiter", "|")
      .option("quote","")
      .format("com.databricks.spark.csv")
      .csv(newRawLocation)

    logger.info("file written in gs location " + newRawLocation)

    val blobs : Page[Blob] = storage.list("batch_ingestion_bucket",
                             Storage.BlobListOption.prefix("RAW/"+ rawLocation))

    var partNameFinal = "null"
    import scala.collection.JavaConversions._
    for (fileList <- blobs.iterateAll) {

      logger.info("Actual file list " + fileList)
      val partName = fileList.getName
      logger.info("filename in blob " + partName)

      if (partName.contains("part")) {
        partNameFinal = "gs://batch_ingestion_bucket/" + partName
        logger.info("Part name file from loop :  " + partNameFinal)
      }
    }

    val srcPath = new Path(partNameFinal)
    val destPath = new Path(newRawLocation + fileName)

    logger.info("source path " + srcPath)
    logger.info("dest path " + destPath)

    val partFileName = srcPath.getName

    logger.info("Part file name is :" +  partFileName)

    srcPath.getFileSystem(conf).rename(new Path(newRawLocation + partFileName), destPath)

    logger.info("Part file is renamed to original file name successfully")

    MoveToSchema.writeToSchema(df,fileName,opcadate, table, dsName)

 }

}
