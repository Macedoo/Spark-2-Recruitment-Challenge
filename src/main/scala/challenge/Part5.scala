package challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part5(df: DataFrame) {
  private var metricsDF: DataFrame = _

  def execute(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val explodedDF = df.withColumn("Genre", explode($"Genres"))

    metricsDF = explodedDF
      .groupBy("Genre")
      .agg(
        countDistinct("App").alias("Count"),
        avg("Rating").alias("Average_Rating"),
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      )

    metricsDF
  }
}
