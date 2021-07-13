package com.ingestion.spark.gcp

import com.google.cloud.storage.StorageOptions
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger


trait Context {

  val sparkConf = new SparkConf().setAppName("Bip_INGESTION_GCP").set("spark.executor.extraClassPath","./")
  val spark = SparkSession.builder().config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery_2.12").enableHiveSupport().getOrCreate()

  spark.conf.set("hive.exec.dynamic.partition", "true")
  spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

  val storage = StorageOptions.newBuilder().setProjectId("dcs-parent-project").build().getService()

  val conf = spark.sparkContext.hadoopConfiguration
}
