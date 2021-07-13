package com.ingestion.spark.gcp

import com.google.api.gax.paging.Page
import com.google.cloud.storage.Storage.{BlobListOption, CopyRequest}
import com.google.cloud.storage.{Blob, CopyWriter, Storage}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{input_file_name, monotonically_increasing_id}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger

import scala.collection.mutable.MutableList
import com.google.cloud.storage.BlobId


object ReadFile extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def main(args : Array[String]) : Unit = {

    val stagingPath = args {
      0
    }
    val rawPath = args {
      1
    }
    val runDate = args {
      2
    }
    val paramPath = args {
      3
    }

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Exactly 4 arguments are required: <inputPath> <outputPath>")
    }

    val PROJECT_ID = ""
    val BUCKET_NAME = " "
    val OBJECT_NAME = " "
    var trailer = "null"

    logger.info("Reading a param file and loading it into big query ")

    val paramDF = spark.sqlContext.read.option("multiline", "true").option("header", "true")
      .option("delimiter", "~").option("quote", "").csv(paramPath)

    paramDF.show(5, false)

    paramDF.cache()

    logger.info("Param dataframe created and now loading it into big query dataset table")

    //paramDF.write.mode("APPEND").format("bigquery").option("temporaryGcsBucket","batch_ingestion_bucket").option("table", "schema_param_table.param_prop_tb").save()

    //logger.info("Data written to big query table")

    logger.info("Read datafile from google cloud storage and create a dataframe")

    val stagblobs: Page[Blob] = storage.list("batch_ingestion_bucket",
      Storage.BlobListOption.prefix("STAGING/"))

    var arr = MutableList[String]()

    import scala.collection.JavaConversions._
    for (stagFileList <- stagblobs.getValues) {

      val filePath = stagFileList.getName

      logger.info(" file Path found " + filePath)

      arr += filePath


    }

    logger.info("Array final value: " + arr)

    for (i <- 1 until arr.length) {

      val stagPath = "gs://batch_ingestion_bucket/" + arr(i)

      logger.info("Staging file in process currently : " + stagPath)

      val stagFileName: String = new Path(stagPath).getName

      logger.info("new FileName " + stagFileName)

      //val headerValue = paramDF.select(paramDF("HeaderCheckValue")).where(paramDF("FileName") === (stagFileName)).distinct().collect().mkString

      val fileDF = paramDF.filter((col("FileName").rlike("Customer_Details*")))

      fileDF.show()

      val headerValue = fileDF.select(fileDF("HeaderCheckValue")).collect().mkString.replaceAll("[\\[\\]]", "")
      val trailerValue = fileDF.select(fileDF("TrailerCheckValue")).collect().mkString.replaceAll("[\\[\\]]", "")
      val fileDelimiter = fileDF.select(fileDF("FileDelimiter")).collect().mkString.replaceAll("[\\[\\]]", "")
      val databaseName = fileDF.select(fileDF("DatasetName")).collect().mkString.replaceAll("[\\[\\]]", "")
      val tableName = fileDF.select(fileDF("Tablename")).collect().mkString.replaceAll("[\\[\\]]", "")


      logger.info("headerValue  " + headerValue + "  trailerValue : " + trailerValue + "  file delimiter " + fileDelimiter
        + "  dataset Name  " + databaseName + "  table name " + tableName)

      var stagingDF = spark.emptyDataFrame

      if (!("NA").equalsIgnoreCase(fileDelimiter)) {

        val df = spark.read.format("bigquery")
          .option("table", databaseName + "." + tableName)
          .option("filter", "Ingestion_Date < CURRENT_TIMESTAMP()")
          .option("query", "select * from  " + databaseName + "." + tableName + "  limit 1")
          .load().cache()

        df.show()

        val newDF = df.drop("BrandId").drop("DeleteFlag").drop("FileName")
          .drop("Ingestion_Date")

        val schema = newDF.schema

        newDF.printSchema()

        if (headerValue.equalsIgnoreCase("Column_Name")) {
          logger.info("The header coming in file is file schema")

          stagingDF = spark.sqlContext.read.option("multiline", "true").option("header", "true").schema(schema).option("inferSchema", "true")
            .option("delimiter", ",").option("quote", "\"").csv(stagPath)

        } else {

          stagingDF = spark.sqlContext.read.option("multiline", "true").option("delimiter", ",").schema(schema).option("inferSchema", "true").option("quote", "\"").csv(stagPath)

        }

      }

      stagingDF.printSchema()

      val newStagingDF = stagingDF.withColumn("index", monotonically_increasing_id())
      newStagingDF.show()

      var finalDF = spark.emptyDataFrame

      var rowCount: Long = stagingDF.count()
      logger.info("Row count from df " + rowCount)

      if (!headerValue.equalsIgnoreCase("Column_Name")
        && (!headerValue.equalsIgnoreCase("NA")) &&
        (!trailerValue.equalsIgnoreCase("NA"))) {
        rowCount = rowCount - 2
        logger.info("final row count is " + rowCount)

        val trailer = getFooter(stagingDF)
        logger.info("Trailer value :" + trailer)
        footerValidation(trailer, rowCount, stagFileName, stagPath)
        finalDF = removeHeader(newStagingDF)
        finalDF = removeFooter(finalDF)

      }
      else if (headerValue.equalsIgnoreCase("Column_Name")
        && !(trailerValue.equalsIgnoreCase("NA"))) {
        rowCount = rowCount - 1
        logger.info("final row count is " + rowCount)
        val trailer = getFooter(stagingDF)
        logger.info("Trailer value :" + trailer)
        finalDF = removeFooter(newStagingDF)
      }
      else if (headerValue.equalsIgnoreCase("NA")
        && !(trailerValue.equalsIgnoreCase("NA"))) {
        rowCount = rowCount - 1
        logger.info("final row count is " + rowCount)
        val trailer = getFooter(stagingDF)
        logger.info("Trailer value :" + trailer)
        footerValidation(trailer, rowCount, stagFileName, stagPath)
        finalDF = removeFooter(newStagingDF)
      }

      else (headerValue.equalsIgnoreCase("NA") && trailerValue.equalsIgnoreCase("NA"))
      rowCount


      val rawDF = finalDF.drop("index")
      rawDF.show()
      moveToRaw.writeToRaw(rawDF, rawPath, runDate, stagFileName, tableName, databaseName)

    }
  }

  def getFooter(df : DataFrame) : String = {

    val footer : Array[Row] = df.tail(1)

    var finalFooter = footer.mkString

    finalFooter = finalFooter.replaceAll("[\\[\\]]", "")

    logger.info("Last row of the dataframe is " + finalFooter)

    finalFooter

  }

  def removeFooter(df:DataFrame) : DataFrame = {

    val last : Long = df.agg(max("index")).collect()(0)(0).asInstanceOf[Long]

    logger.info("Max number " + last)

    val filtered  = df.filter(col("index") =!= (last))

    filtered.show()

    filtered
  }

  def removeHeader(df : DataFrame) : DataFrame = {

    val head = df.head()
    logger.info("Header " + head )
    val withoutHeader = df.filter(col("index") =!= 0)
    withoutHeader.show()

    withoutHeader

  }

  def footerValidation(footer : String , recordCount : Long, fileName : String, stag : String) : String = {

    var arr : Array[String] = null
    arr = footer.split("\\,")

    logger.info("record count " + recordCount)

    val x = arr.contains(recordCount.toString)

    logger.info("Array value " + arr.mkString(","))

    logger.info("Boolean value : " + x.toString)

    logger.info("Stag path is : " + stag + "  and filename is " + fileName)

    if (x) {
      logger.info("return footer value " + footer)
      footer
    } else {

      val badBlob = "BAD/" + fileName

      val request = CopyRequest.newBuilder().setSource(BlobId.of("batch_ingestion_bucket", "STAGING/" + fileName))
        .setTarget(BlobId.of("batch_ingestion_bucket", badBlob)).ensuring(true).build()

      val copyWriter = storage.copy(request)
      val copied : Blob = copyWriter.getResult

      logger.info("copied result is " + copied)

      if(copyWriter.isDone) {
        logger.info("Copying file from staging to bad successful...now deleting the file at staging ")
        val stagBlob = BlobId.of("batch_ingestion_bucket", "STAGING/" +  fileName)
        val deleted = storage.delete(stagBlob)
        logger.info("File deleted from staging location")
      }

      throw new Exception("record count does not match with trailer value")
    }


  }

}
