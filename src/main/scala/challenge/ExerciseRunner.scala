package challenge

import org.apache.spark.sql.SparkSession

object ExerciseRunner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark 2 Challenge")
      .master("local[*]")
      .getOrCreate()

    // Load the datasets into DataFrames
    val dfGooglePlayStore = spark.read.format("csv").option("header", "true")
      .load("datasets/googleplaystore.csv")
    val dfGooglePlayStoreUserReviews = spark.read.format("csv").option("header", "true")
      .load("datasets/googleplaystore_user_reviews.csv")

    val outputFolder = "output_files/"

    //Execute exercises and save results

    val part1 = Part1(dfGooglePlayStoreUserReviews)
    val df_1 = part1.execute(spark)

    val part2 = Part2(dfGooglePlayStore)
    val df_2 = part2.execute(spark)

    val part3 = Part3(dfGooglePlayStore)
    val df_3 = part3.execute(spark)

    val part4 = Part4(df_1,df_3)
    val df_4 = part4.execute(spark)

    // For Part 5, I used df_4 instead of df_3 as per the exercise instructions. df_4 is a result of the union between
    // df_3 and the column 'Average_Sentiment_Polarity' from df_1. This approach helped avoid redundant joins and
    // optimized the process.
    val part5 = Part5(df_4)
    val df_5 = part5.execute(spark)


    // Save results to output paths

    // Used coalesce(1) to repartition to a single file as asked in the challenge although for larger datasets it could
    // impact performance
    df_2.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "§")
      .option("encoding", "UTF-8")
      .csv(outputFolder+"/best_apps.csv")

    // Save the metrics DataFrame as a Parquet file with gzip compression

    df_4.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("compression", "gzip")
      .parquet(outputFolder + "/googleplaystore_cleaned.parquet")


    df_5.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet(outputFolder + "/googleplaystore_metrics.parquet")

    spark.stop()
  }
}
