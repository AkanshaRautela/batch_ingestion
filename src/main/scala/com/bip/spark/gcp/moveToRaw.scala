package com.bip.spark.gcp

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

  def writeToRaw(df:DataFrame,rawPath : String, opcadate : String) = {

    logger.info("Writing the df into raw google cloud storage " + rawPath)

    val sdf = new SimpleDateFormat("yyyyMMdd")

    val storage = StorageOptions.newBuilder().setProjectId("dcs-parent-project").build().getService()


    var year = opcadate.substring(0,4).toInt
    var month = opcadate.substring(4,6).toInt
    var day = opcadate.substring(6,8).toInt

    var fileName = df.select(df("FileName")).distinct().collect().mkString

    fileName = fileName.replaceAll("[\\[\\]]", "")

    logger.info("Filename under process is " + fileName)

    val conf = spark.sparkContext.hadoopConfiguration

    val fs = FileSystem.get(conf)

    val newFileName : String = new Path(fileName).getName

    logger.info("new FileName " + newFileName)

    val newRawLocation = rawPath + "GOOD" + "/year=" + year + "/" + "month=" + month + "/" + "day=" + day + "/"

    logger.info("New Raw location required is " + newRawLocation)

    df.repartition(1).write
      .mode("APPEND")
      .option("delimiter", "|")
      .option("quote","")
      .format("com.databricks.spark.csv")
      .csv(newRawLocation)

    logger.info("file written in gs location " + newRawLocation)

    val srcPath = new Path(newRawLocation + "/part*")
    val destPath = new Path(newRawLocation + newFileName)

    logger.info("source path " + srcPath)
    logger.info("dest path " + destPath)

    //val partName = srcPath.getName

//    val bucket = storage.get("batch_ingestion_bucket")
//    val blobListOptions = Storage.BlobListOption.prefix("part")
//
//    logger.info("Part filename is " + partName)
//
     val blobs = storage.list("batch_ingestion_bucket")

    var partName = "null"
    import scala.collection.JavaConversions._
    for (fileList <- blobs.iterateAll) {

      partName = fileList.getName
      logger.info("filename in blob " + partName)
    }

//    val blob = storage.get("batch_ingestion_bucket", newRawLocation + partName)
//    val copyWriter = blob.copyTo("batch_ingestion_bucket", newRawLocation + newFileName)
//
//    copyWriter.getResult()
//
//    blob.delete()

    //fs.rename(new Path(newRawLocation + partName),destPath)
    //srcPath.getFileSystem(conf).rename(new Path(newRawLocation + partName), destPath)

    logger.info("Part file is renamed to original file name successfully")

    //MoveToSchema.writeToSchema(newFileName,opcadate)

 }

}
