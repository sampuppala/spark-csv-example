package org.spark.examples

import org.apache.spark.sql.SparkSession
import scala.io.Source

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .load("Salaries.csv")
    df.show()
  }
}