import challenge.Part1
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

/**
 * Part1Test
 * This test suite validates the functionality of the Part1 class.
 * It verifies the correctness of DataFrame transformations and the expected data types.
 */
class Part1Test extends FunSuite with BeforeAndAfterAll {

  // SparkSession for testing
  private var spark: SparkSession = _

  // Test DataFrames
  private var testDF: DataFrame = _
  private var expectedDF: DataFrame = _

  // Setup before running tests
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    // Define schema for test data
    val schema = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Translated_Review", StringType, nullable = true),
        StructField("Sentiment", StringType, nullable = true),
        StructField("Sentiment_Polarity", StringType, nullable = true),
        StructField("Sentiment_Subjectivity", StringType, nullable = true)
      )
    )

    // Test data
    val data = Seq(
      Row("10 Best Foods for You", "This help eating healthy exercise regular basis", "Positive", "0.25",
        "0.28846153846153844"),
      Row("10 Best Foods for You", "Best idea us", "Positive", "1.0", "0.3"),
      Row("10 Best Foods for You", "Amazing", "Positive", "0.6000000000000001", "0.9"),
      Row("8 Ball Pool", "Amazing", "Positive", null, null),
      Row("10 Best Foods for You", "Looking forward app,", "Neutral", "0.0", "0.0")
    )

    // Expected schema for transformed data
    val expectedSchema = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Average_Sentiment_Polarity", DoubleType, nullable = false)
      )
    )

    // Expected transformed data
    val expectedData = Seq(
      Row("8 Ball Pool", 0.0),
      Row("10 Best Foods for You", 0.4625)
    )

    // Create DataFrames from test data and expected data

    testDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    expectedDF= spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema)

  }


  test("Test DataFrame transformation for Part 1") {
    val part1 = Part1(testDF)
    val resultDF = part1.execute(spark)

    assert(resultDF.columns.length === 2)
    assert(resultDF.columns.contains("App"))
    assert(resultDF.columns.contains("Average_Sentiment_Polarity"))
  }

  test("Test Data Types for Part 1") {
    val part1 = Part1(testDF)
    val resultDF = part1.execute(spark)

    assert(resultDF.schema("App").dataType == StringType)
    assert(resultDF.schema("Average_Sentiment_Polarity").dataType == DoubleType)
  }

  test("Test to match expected output for Part 1") {
    val part1 = Part1(testDF)
    val resultDF = part1.execute(spark)

    assert(resultDF.exceptAll(expectedDF).count() == 0 && expectedDF.exceptAll(resultDF).count() == 0)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }
}
