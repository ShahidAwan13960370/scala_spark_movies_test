package com.test.movies
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import com.test.movies.Util.getFilePath
object Loader extends SparkSessionWrapper {

  def loadDataFrame(dir: String, file: String, schema: StructType):  DataFrame ={
    spark.read
      .format("csv")
      .option("delimiter","::")
      .option("quote","")
      .schema(schema)
      .load(getFilePath(dir = dir, file = file))
  }

  def loadMovies(inputDir: String): DataFrame = {
    val schema = StructType(Array(
      StructField("MovieID", IntegerType, true),
      StructField("Title", StringType, true),
      StructField("Genre", StringType, true)
    ))

    loadDataFrame(dir = inputDir,
      file = "movies.dat",
      schema = schema)
  }

  def loadUsers(inputDir: String) : DataFrame = {
    val schema = StructType(Array(
      StructField("UserID", IntegerType, true),
      StructField("Gender", StringType, true),
      StructField("Age", IntegerType, true),
      StructField("Occupation", IntegerType, true),
      StructField("Zip-code", StringType, true)
    ))
    loadDataFrame(dir = inputDir,
      file = "users.dat",
      schema = schema)
  }

  def loadRatings(inputDir: String): DataFrame = {
    val schema = StructType(Array(
      StructField("UserID", IntegerType, true),
      StructField("MovieID", IntegerType, true),
      StructField("Rating", IntegerType, true),
      StructField("TimeStamp", IntegerType, true)
    ))
    loadDataFrame(dir = inputDir,
      file = "ratings.dat",
      schema = schema)
  }
}
