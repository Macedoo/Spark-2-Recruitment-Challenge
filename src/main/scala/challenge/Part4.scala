
package challenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class Part4(df1: DataFrame, df3: DataFrame) {
  private var joinedDF: DataFrame = _
  def execute(spark: SparkSession): DataFrame = {

    // Perform a left join on 'App'
    val joinedDF = df3.join(df1.select("App", "Average_Sentiment_Polarity"), Seq("App"), "left")

    // Select the columns from df3 and Average_Sentiment_Polarity from df1
    val df4 = joinedDF.select(df3.col("*"), df1.col("Average_Sentiment_Polarity")).orderBy("App")

    df4
  }
}
