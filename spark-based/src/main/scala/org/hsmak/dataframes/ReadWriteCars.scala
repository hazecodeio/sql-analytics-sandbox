package org.hsmak.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ReadWriteCars extends App {

  //turn off Logging
  Logger.getLogger("org").setLevel(Level.OFF)


  val base_data_dir = s"file://${System.getProperty("user.dir")}/_data/car-data"


  /** ******************************************************
    * ############ Creating SparkSession ###########
    * ******************************************************/

  val spark = SparkSession
    .builder
    .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
    .appName("DatasetRunner")
    .getOrCreate()


  /** ******************************************************
    * ############ Creating DataFrames from CSVs ###########
    * ******************************************************/


  val carMileageDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$base_data_dir/cars.csv")


  println("carMileageDF has " + carMileageDF.count() + " rows")
  carMileageDF.show(5)
  carMileageDF.printSchema()


  /** ******************************************************
    * ############ Writing DataFrames back #################
    * ******************************************************/

  //Write DF back in CSV format
  carMileageDF.write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .csv(s"${base_data_dir}/out/cars-out-csv")


  //Write DF back in Parquet format
  carMileageDF.write
    .mode(SaveMode.Overwrite)
    .option("header", true)
    .partitionBy("year")
    .parquet(s"${base_data_dir}/out/cars-out-pqt")

}
