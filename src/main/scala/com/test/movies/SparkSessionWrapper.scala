package com.test.movies
import org.apache.spark.sql.SparkSession


trait SparkSessionWrapper {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("movies test")
    .getOrCreate()

}
