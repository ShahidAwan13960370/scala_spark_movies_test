package com.test.movies
import org.apache.spark.sql.DataFrame
import com.test.movies.Util.getFilePath
object Writer extends SparkSessionWrapper {
  def writeDataFrame(outputDir: String, file: String, df: DataFrame): Unit ={
    df.write.mode("overwrite").parquet(getFilePath(dir = outputDir, file = file))
  }
}
