package challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part1(data: DataFrame) {
  private var result: DataFrame = _

  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Calculate the average sentiment polarity grouped by 'App'
    result = data.groupBy("App")
      .agg(avg($"Sentiment_Polarity").alias("Average_Sentiment_Polarity"))
      .na.fill(0, Seq("Average_Sentiment_Polarity"))

    result
  }
}
