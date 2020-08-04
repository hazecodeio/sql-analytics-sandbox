package org.hsmak.dataframes

import org.apache.spark.sql.SparkSession

/**
  * @author ${user.name}
  */
object DatasetRunner {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .master("local[*]") // ToDO: Which config takes precedence? MainApp hard-coded or spark-submit argument; mvn exec:exec?
      .appName("DatasetRunner")
      .getOrCreate()

    import spark.implicits._

    val filePath = s"file://${System.getProperty("user.dir")}/_data/house_prices/test.csv"
    // via SparkSQL
    val dataset = spark.read.textFile(filePath)
      .flatMap(line => line.split(","))
      .map(w => (w, 1))//why can't reduceByKey on Dataset???

    dataset.show()

    val df = dataset.toDF("words", "frequency")
    df.show()


    spark.stop()
  }

}
