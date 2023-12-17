import challenge.Part5
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}

/**
 * Part5Test: Contains test suites to validate the functionality of the `Part5` class.
 * The tests cover various aspects of the `execute` method in the `Part5` class.
 */
class Part5Test extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var df: DataFrame = _
  private var expectedDF: DataFrame = _

  // Initialization of SparkSession and sample test data
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    // Sample DataFrame
    val schema = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Categories", ArrayType(StringType), nullable = true),
        StructField("Rating", DoubleType, nullable = true),
        StructField("Reviews", LongType, nullable = true),
        StructField("Size", DoubleType, nullable = true),
        StructField("Installs", StringType, nullable = true),
        StructField("Type", StringType, nullable = true),
        StructField("Price", DoubleType, nullable = true),
        StructField("Content_Rating", StringType, nullable = true),
        StructField("Genres", ArrayType(StringType), nullable = true),
        StructField("Last_Updated", DateType, nullable = true),
        StructField("Current_Version", StringType, nullable = true),
        StructField("Minimum_Android_Version", StringType, nullable = true),
        StructField("Average_Sentiment_Polarity", DoubleType, nullable = false)
      )
    )

    val data = Seq(
      Row("App1", Array("Category1", "Category2"), 4.0, 1001L, 2.3, "1000+", "Paid", 1.791, "Everyone",
        Array("Genre1", "Genre2"), java.sql.Date.valueOf("2017-10-07"), "1.0", "4.0", 0.5),
      Row("App2", Array("Category3"), 3.5, 500L, 0.808, "100+", "Paid", 0.891, "Teen",
        Array("Genre1","Genre3"), java.sql.Date.valueOf("2016-10-21"), "1.1", "4.2", 0.8)
    )

    df = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    // Expected DataFrame after execute() method
    val expectedSchema = StructType(
      List(
        StructField("Genre", StringType, nullable = true),
        StructField("Count", LongType, nullable = false),
        StructField("Average_Rating", DoubleType, nullable = true),
        StructField("Average_Sentiment_Polarity", DoubleType, nullable = true)
      )
    )

    val expectedData = Seq(
      Row("Genre1", 2L, 3.75, 0.65),
      Row("Genre2", 1L, 4.0, 0.5),
      Row("Genre3", 1L, 3.5, 0.8)
    )

    expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )
  }

  // Test to validate the execute method of Part5
  test("Test execute method of Part5 - Grouping and Aggregation") {
    val part5 = Part5(df)
    val resultDF = part5.execute(spark)

    resultDF.show()
    expectedDF.show()

    assert(resultDF.count() == 3) // Check if all genres are present in the result
    assert(resultDF.columns.sameElements(expectedDF.columns)) // Check if columns match
    assert(resultDF.collect().sameElements(expectedDF.collect())) // Check if content matches
  }

  // Test to validate data types in the resulting DataFrame
  test("Test Data Types for Part5") {
    val part5 = Part5(df)
    val resultDF = part5.execute(spark)

    // Check data types of columns in the resultDF
    assert(resultDF.schema.fields(0).dataType == StringType)
    assert(resultDF.schema.fields(1).dataType == LongType)
    assert(resultDF.schema.fields(2).dataType == DoubleType)
    assert(resultDF.schema.fields(3).dataType == DoubleType)

  }

  // Test to validate if the DataFrame is written to the expected file
  test("Test DataFrame is written to the expected file") {
    val part5 = Part5(df)
    val resultDF = part5.execute(spark)

    val outputPath = "output_files/googleplaystore_metrics.parquet"

    // Write the DataFrame to a CSV file
    resultDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("compression", "gzip")
      .parquet(outputPath)

      // Check if the file exists at the specified location
      val fileExists = Files.exists(Paths.get(outputPath))

      // Assert that the file exists
      assert(fileExists)

  }

  // Cleanup after all tests are done
  override def afterAll(): Unit = {
    spark.stop()
  }
}
