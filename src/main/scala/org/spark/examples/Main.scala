package org.spark.examples

import org.apache.spark.sql.SparkSession

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

    //Seq("Id", StringType).

    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("Salaries.csv").toDF()
    df.select("JobTitle", "EmployeeName", "BasePay","OvertimePay","OtherPay","Benefits",
      "TotalPay","TotalPayBenefits","Year", "Agency").show(100)
    df.groupBy("JobTitle").count().show()
    df.groupBy("Year").count().show()
    //df.show()
  }
}