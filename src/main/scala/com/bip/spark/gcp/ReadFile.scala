package com.bip.spark.gcp


import com.bip.spark.gcp.moveToRaw.storage
import com.google.api.gax.paging.Page
import com.google.cloud.storage.{Blob, Storage}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{input_file_name, monotonically_increasing_id}
import org.apache.spark.sql.functions._
import org.apache.log4j.Logger


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

    //    val options = StorageOptions.newBuilder()
    //      .setProjectId(PROJECT_ID)
    //      .setCredentials(GoogleCredentials.fromStream(
    //        new FileInputStream(paramPath))).build()
    //
    //    val storage = options.getService
    //    val blob = storage.get(BUCKET_NAME, OBJECT_NAME)
    //
    //    val fileContent : String = new String(blob.getContent())

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

    val df = spark.sqlContext.read.option("multiline", "true")
      .option("delimiter", ",").option("quote", "\"").csv(stagingPath).withColumn("FileName", input_file_name())

    logger.info("spark dataframe created ")

    df.show(10, false)

    var fileName = df.select(df("FileName")).distinct().collect().mkString

    fileName = fileName.replaceAll("[\\[\\]]", "")

    logger.info("Filename under process is " + fileName)

    val stagFileName: String = new Path(fileName).getName

    logger.info("new FileName " + stagFileName)

    //val headerValue = df.select(df("HeaderCheckValue")).where(df("FileName") === (stagFileName)).distinct().collect().mkString

    val fileDF = paramDF.filter(col("FileName").contains(stagFileName))

    fileDF.show()

    val headerValue = fileDF.select(fileDF("HeaderCheckValue")).collect().mkString.replaceAll("[\\[\\]]", "")
    val trailerValue = fileDF.select(fileDF("TrailerCheckValue")).collect().mkString.replaceAll("[\\[\\]]", "")

    logger.info("headerValue  " + headerValue + "  trailerValue : " + trailerValue)

    var stagingDF = spark.emptyDataFrame

    if (headerValue.equalsIgnoreCase("Column_Name")) {
      logger.info("The header coming in file is file schema")

      stagingDF = spark.sqlContext.read.option("multiline", "true").option("header", "true")
        .option("delimiter", ",").option("quote", "\"").csv(stagingPath)

    } else {

      stagingDF = spark.sqlContext.read.option("multiline", "true").option("delimiter", ",").option("quote", "\"").csv(stagingPath)

    }



    val newStagingDF = stagingDF.withColumn("index",monotonically_increasing_id() )
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
      footerValidation(trailer, rowCount)
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
      footerValidation(trailer,rowCount)
      finalDF = removeFooter(newStagingDF)
    }

    else (headerValue.equalsIgnoreCase("NA") && trailerValue.equalsIgnoreCase("NA"))
      rowCount


    val rawDF = finalDF.drop("index")
    rawDF.show()
    moveToRaw.writeToRaw(rawDF, rawPath, runDate,stagFileName)

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

  def footerValidation(footer : String , recordCount : Long) : String = {

    var arr : Array[String] = null
    arr = footer.split("\\,")

    logger.info("record count " + recordCount)

    val x = arr.contains(recordCount.toString)

    logger.info("Array value " + arr.mkString(","))

    logger.info("Boolean value : " + x.toString)

    if (x) {
      logger.info("return footer value " + footer)
      footer
    } else
      throw new Exception("record count does not match with trailer value")


  }

}
