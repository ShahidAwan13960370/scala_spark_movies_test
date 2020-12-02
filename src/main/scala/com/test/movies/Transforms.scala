package com.test.movies
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.test.movies.Loader._
import com.test.movies.Writer.{spark, _}
import org.apache.spark.sql.expressions.Window
import spark.implicits._

object Transforms {

  def getMovieRatings(moviesDf: DataFrame, ratingsDf: DataFrame): DataFrame = {
    val movieRatingJoinedDf = moviesDf.join(ratingsDf,
      moviesDf.col("MovieID") === ratingsDf.col("MovieID"))
      .groupBy(moviesDf.col("MovieID"))
      .agg(
        max(ratingsDf.col("Rating")).as("max_rating"),
        min(ratingsDf.col("Rating")).as("min_rating"),
        avg(ratingsDf.col("Rating")).as("average_rating")
      )
    movieRatingJoinedDf.alias("mj").join(moviesDf.alias("m"),
      movieRatingJoinedDf.col("MovieID") === moviesDf.col("MovieID"))
      .select("mj.MovieID", "m.Title", "m.Genre", "mj.max_rating", "mj.min_rating", "mj.average_rating")
  }


  def getUserMovieRatings(ratingsDf: DataFrame, usersDf: DataFrame, count: Int): DataFrame = {
    val userRatings = usersDf.alias("u").join(ratingsDf.alias("r"),
      usersDf("UserID") === ratingsDf("UserID"))
      .orderBy(usersDf("UserID"), ratingsDf("Rating").desc)
      .select("u.UserID", "r.MovieID", "r.Rating")
    val userIdRatingWindow = Window.partitionBy($"UserID").orderBy($"Rating".desc)
    userRatings.withColumn("row_no", row_number.over(userIdRatingWindow)).where($"row_no" <= count).drop("row_no")
  }

  def run(inputDir: String, outputDir: String): Unit = {
    val movies = loadMovies(inputDir = inputDir)
    val ratings = loadRatings(inputDir = inputDir)
    val users = loadUsers(inputDir = inputDir)

    val movieRatings = getMovieRatings(moviesDf = movies, ratingsDf = ratings)
    movieRatings.show()

    val userRating = getUserMovieRatings(ratingsDf = ratings, usersDf = users, count = 3)
    userRating.show()

    writeDataFrame(outputDir = outputDir, file = "movies", df = movies)
    writeDataFrame(outputDir = outputDir, file = "users", df = users)
    writeDataFrame(outputDir = outputDir, file = "ratings", df = ratings)
    writeDataFrame(outputDir = outputDir, file = "movie_ratings", df = movieRatings)
    writeDataFrame(outputDir = outputDir, file = "user_ratings", df = userRating)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2){
      println("required two arguments <input_dir> <output_dir>")

    }else {
      run(inputDir = args(0), outputDir = args(1))
    }
  }
}
