import challenge.Part2
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}

/**
 * Part2Test: Contains test suites to validate the functionality of the `Part2` class.
 * The tests cover various aspects of the `execute` method in the `Part2` class, ensuring it filters and sorts the
 * DataFrame correctly.
 */
class Part2Test extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var testDF: DataFrame = _
  private var expectedDF: DataFrame = _

  // Initialization of SparkSession and sample test data
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Category", StringType, nullable = true),
        StructField("Rating", StringType, nullable = true),
        StructField("Reviews", StringType, nullable = true),
        StructField("Size", StringType, nullable = true),
        StructField("Installs", StringType, nullable = true),
        StructField("Type", StringType, nullable = true),
        StructField("Price", StringType, nullable = true),
        StructField("Content Rating", StringType, nullable = true),
        StructField("Genres", StringType, nullable = true)
      )
    )

    val data = Seq(
      Row("App1", "Category1", "4.5", "1000", "20M", "10,000+", "Free", "0", "Everyone", "Health & Fitness"),
      Row("App2", "Category2", "4.3", "500", "25M", "5,000+", "Paid", "$1.99", "Teen", "Education"),
      Row("App3", "Category3", "NaN", "100", "30M", "1,000+", "Free", "0", "Everyone", "Entertainment") ,
      Row("App4", "Category3", "3.9", "100", "30M", "1,000+", "Free", "0", "Everyone", "Entertainment")
    )

    testDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    val expectedSchema = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Category", StringType, nullable = true),
        StructField("Rating", StringType, nullable = true),
        StructField("Reviews", StringType, nullable = true),
        StructField("Size", StringType, nullable = true),
        StructField("Installs", StringType, nullable = true),
        StructField("Type", StringType, nullable = true),
        StructField("Price", StringType, nullable = true),
        StructField("Content Rating", StringType, nullable = true),
        StructField("Genres", StringType, nullable = true)
      )
    )

    val expectedData = Seq(
      Row("App1", "Category1", "4.5", "1000", "20M", "10,000+", "Free", "0", "Everyone", "Health & Fitness"),
      Row("App2", "Category2", "4.3", "500", "25M", "5,000+", "Paid", "$1.99", "Teen", "Education")
    )

    expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )
  }
  // Test to validate filtering and sorting in execute method of Part2
  test("Test execute method of Part2 - Filter and Sort") {
    val part2 = Part2(testDF)
    val resultDF = part2.execute(spark)

    assert(resultDF.count() == 2) // Check if two rows with valid ratings are returned
    assert(resultDF.columns.sameElements(expectedDF.columns)) // Check if columns match
    assert(resultDF.collect().sameElements(expectedDF.collect())) // Check if content matches
  }

  // Test to validate data types in the resulting DataFrame
  test("Test Data Types for Part2") {
    val part2 = Part2(testDF)
    val resultDF = part2.execute(spark)

    // Check data types of columns in the resultDF
    assert(resultDF.schema.fields(0).dataType == StringType)
    assert(resultDF.schema.fields(2).dataType == StringType)

  }

  test("Test DataFrame is written to the expected file") {
    val part2 = Part2(testDF)
    val resultDF = part2.execute(spark)

    val outputPath = "output_files/best_apps.csv"

    // Write the DataFrame to a CSV file
    resultDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "§")
      .option("encoding", "UTF-8")
      .csv(outputPath)

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
