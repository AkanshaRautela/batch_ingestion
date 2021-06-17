package com.bip.spark.gcp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.input_file_name
//import com.google.auth.oauth2.GoogleCredentials
//import com.google.cloud.ReadChannel
//import com.google.cloud.storage.Blob
//import com.google.cloud.storage.Storage
//import com.google.cloud.storage.StorageOptions
//import org.apache.http.client.methods.RequestBuilder.options
import org.apache.log4j.Logger
import org.apache.spark.SPARK_VERSION
import org.apache.spark.rdd.RDD
import com.google.cloud.bigquery.JobInfo.CreateDisposition.CREATE_NEVER
import com.google.cloud.bigquery._
//import com.google.cloud.bigquery.connector.common.{BigQueryClient, BigQueryUtil}

import java.io.FileInputStream


object ReadFile extends Context {

  val logger = Logger.getLogger(this.getClass.getSimpleName)

  def main(args : Array[String]) : Unit = {

    val stagingPath = args{0}
    val rawPath = args{1}
    val runDate = args{2}
    val paramPath= args{3}

    if (args.length != 4) {
      throw new IllegalArgumentException(
        "Exactly 4 arguments are required: <inputPath> <outputPath>")
    }

    val PROJECT_ID= ""
    val BUCKET_NAME = " "
    val OBJECT_NAME = " "

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

    val paramDF = spark.sqlContext.read.option("multiline","true").option("header","true")
      .option("delimiter", "~").option("quote","").csv(paramPath)

    paramDF.show(5,false)

    logger.info("Param dataframe created and now loading it into big query dataset table")

    paramDF.write.mode("APPEND").format("bigquery").option("temporaryGcsBucket","batch_ingestion_bucket").option("table", "schema_param_table.param_prop_tb").save()

    logger.info("Data written to big query table")

    logger.info("Read datafile from google cloud storage and create a dataframe")

    val df = spark.sqlContext.read.option("multiline","true").option("header","true")
      .option("delimiter", "|").option("quote","").csv(stagingPath).withColumn("FileName",input_file_name())

    logger.info("spark dataframe created ")

    df.show(10,false)

    //val trailer  = "5"

    //val withoutFooterDF = removeFooter(df)

//    val rowCount : Long = withoutFooterDF.count()
//    logger.info("Row count from df " + rowCount)
//
//    if (rowCount == trailer.toLong) {
//      logger.info("File Row count and trailer count matched")
//
//      withoutFooterDF.show()
//      //logger.info("check for duplicate rows")
//      moveToRaw.writeToRaw(withoutFooterDF,rawPath,runDate)
//
//    } else {
//      throw new Exception("Record count does'nt match ")
//
//    }

     moveToRaw.writeToRaw(df,rawPath,runDate)


  }

  def getFooter(df : DataFrame) : String = {

    logger.info("Convert into RDD")

    df.createGlobalTempView("temp_table")

    //val trailer = spark.sql("select * from temp_table order by ")

    val trailer = df.rdd.mapPartitionsWithIndex((i,iter) => iter.zipWithIndex.map { case (x,j) => ((i,j),x)}).top(2)(Ordering[(Int,Int)].on(_._1)).headOption.map(_._2)

    val footer = trailer.mkString
    logger.info("Trailer value from df is : " + footer)

    val finalFooter = footer.replaceAll("[\\[\\]]", "")

    logger.info("Trailer value after replacing unwanted braces" + finalFooter)

    finalFooter

  }

  def removeFooter(df:DataFrame) : DataFrame = {

    val rdd = df.rdd
    val schema = df.schema

    val lastPartition = rdd.getNumPartitions -1
    val withoutTrailer = rdd.mapPartitionsWithIndex { (idx, iter) => {
      var reti = iter
      if (idx == lastPartition) {
        val lastPart = iter.toArray
        reti = lastPart.slice(0, lastPart.length - 1).toIterator
      }
      reti
    }
    }

    val newDF = spark.createDataFrame(withoutTrailer,schema)

    newDF
  }

}
