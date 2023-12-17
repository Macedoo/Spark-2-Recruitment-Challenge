import challenge.Part4
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataTypes, DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}
import java.sql.Date

/**
 * Part4Test: Contains test suites to validate the functionality of the `Part4` class.
 * The tests cover various aspects of the `execute` method in the `Part4` class.
 */
class Part4Test extends FunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var df1: DataFrame = _
  private var df3: DataFrame = _
  private var expectedDF: DataFrame = _

  // Initialization of SparkSession and sample test data
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    // Sample DataFrame 1 (df1)
    val schema1 = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Average_Sentiment_Polarity", DoubleType, nullable = false)
      )
    )

    val data1 = Seq(
      Row("App1", 0.5),
      Row("App2", 0.8),
      Row("App3", -0.2)
    )

    df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(data1),
      schema1
    )

    // Sample DataFrame 3 (df3)

    val schema3 = StructType(
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
        StructField("Genres",ArrayType(StringType), nullable = true),
        StructField("Last_Updated", DateType, nullable = true),
        StructField("Current_Version", StringType, nullable = true),
        StructField("Minimum_Android_Version", StringType, nullable = true)
      )
    )

    val data3 = Seq(
      Row("App1", Array("Category1","Category2"), 4.0, 1001L, 2.3, "1000+", "Paid", 1.791, "Everyone",
        Array("Genre1", "Genre2"), Date.valueOf("2017-10-07"), "1.0", "4.0"),
      Row("App2", Array("Category3"), 3.5, 500L, 0.808, "100+", "Paid", 0.891, "Teen",
        Array("Genre3"), Date.valueOf("2016-10-21"), "1.1", "4.2")
    )

    df3 = spark.createDataFrame(
      spark.sparkContext.parallelize(data3),
      schema3
    )

    // Expected DataFrame after execute() method
    val expectedSchema = StructType(
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
        StructField("Genres",ArrayType(StringType), nullable = true),
        StructField("Last_Updated", DateType, nullable = true),
        StructField("Current_Version", StringType, nullable = true),
        StructField("Minimum_Android_Version", StringType, nullable = true),
        StructField("Average_Sentiment_Polarity", DoubleType, nullable = false)
      )
    )

    val expectedData = Seq(
      Row("App1", Array("Category1","Category2"), 4.0, 1001L, 2.3, "1000+", "Paid", 1.791, "Everyone",
        Array("Genre1", "Genre2"), Date.valueOf("2017-10-07"), "1.0", "4.0",0.5),
      Row("App2", Array("Category3"), 3.5, 500L, 0.808, "100+", "Paid", 0.891, "Teen",
        Array("Genre3"), Date.valueOf("2016-10-21"), "1.1", "4.2",0.8)
    )

    expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      expectedSchema
    )
  }

  test("Test DataFrame transformation to meet Part 3 requirements") {
    val part4 = Part4(df1, df3)
    val resultDF = part4.execute(spark)

    // Test for the existence of columns after transformation
    assert(resultDF.columns.length === 14)
    assert(resultDF.columns.contains("App"))
    assert(resultDF.columns.contains("Categories"))
    assert(resultDF.columns.contains("Rating"))
    assert(resultDF.columns.contains("Reviews"))
    assert(resultDF.columns.contains("Size"))
    assert(resultDF.columns.contains("Installs"))
    assert(resultDF.columns.contains("Type"))
    assert(resultDF.columns.contains("Price"))
    assert(resultDF.columns.contains("Content_Rating"))
    assert(resultDF.columns.contains("Genres"))
    assert(resultDF.columns.contains("Last_Updated"))
    assert(resultDF.columns.contains("Current_Version"))
    assert(resultDF.columns.contains("Minimum_Android_Version"))
    assert(resultDF.columns.contains("Average_Sentiment_Polarity"))
  }

  // Test to validate the execute method of Part4
  test("Test execute method of Part4 - Join and Column Selection") {
    val part4 = Part4(df1, df3)
    val resultDF = part4.execute(spark)

    assert(resultDF.count() == 2) // Check if all rows are present after join
    assert(resultDF.columns.sameElements(expectedDF.columns)) // Check if columns match
    assert(resultDF.collect().sameElements(expectedDF.collect())) // Check if content matches
  }

  // Test to validate data types in the resulting DataFrame
  test("Test Data Types") {
    val part4 = Part4(df1, df3)
    val resultDF = part4.execute(spark)

    assert(resultDF.schema("App").dataType == StringType)
    assert(resultDF.schema("Categories").dataType == DataTypes.createArrayType(StringType))
    assert(resultDF.schema("Rating").dataType == DoubleType)
    assert(resultDF.schema("Reviews").dataType == LongType)
    assert(resultDF.schema("Size").dataType == DoubleType)
    assert(resultDF.schema("Installs").dataType == StringType)
    assert(resultDF.schema("Type").dataType == StringType)
    assert(resultDF.schema("Price").dataType == DoubleType)
    assert(resultDF.schema("Content_Rating").dataType == StringType)
    assert(resultDF.schema("Genres").dataType == DataTypes.createArrayType(StringType))
    assert(resultDF.schema("Last_Updated").dataType == DateType)
    assert(resultDF.schema("Current_Version").dataType == StringType)
    assert(resultDF.schema("Minimum_Android_Version").dataType == StringType)
    assert(resultDF.schema("Average_Sentiment_Polarity").dataType == DoubleType)
  }

  // Test to validate if the DataFrame is written to the expected file
  test("Test DataFrame is written to the expected file") {
    val part4 = Part4(df1, df3)
    val resultDF = part4.execute(spark)

    val outputPath = "output_files/googleplaystore_cleaned.parquet"

    // Write the DataFrame to a parquet file
    resultDF.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
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
