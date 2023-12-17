package challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part2(dfGooglePlayStore: DataFrame) {
  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val bestApps = dfGooglePlayStore
      .filter($"Rating".isNotNull)  // Exclude rows with null values in "Rating"
      .filter($"Rating" =!= "NaN") // Filter out 'NaN' strings from Rating column
      .filter($"Rating" >= 4.0)
      .filter($"Rating" >= 4.0)
      .sort(desc("Rating"))

    bestApps
  }

}
