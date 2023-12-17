import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll
import challenge.Part3
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}
import java.sql.Date

/**
 * Part3Test Class Documentation
 *
 * The `Part3Test` class serves to test the functionalities and transformations implemented in the `Part3` class.
 * This suite of tests ensures that the data transformations meet specific requirements and produce expected outputs.
 */
class Part3Test extends FunSuite with BeforeAndAfterAll  {
  // Test DataFrame
  private var testDF: DataFrame = _
  private var expectedDF: DataFrame = _
  private var spark : SparkSession = _

  /**
   * Initialization
   */
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    // Load test data into a DataFrame
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
        StructField("Genres",StringType, nullable = true),
        StructField("Last Updated", StringType, nullable = true),
        StructField("Current Ver", StringType, nullable = true),
        StructField("Android Ver", StringType, nullable = true)
      )
    )

    val new_schema = StructType(
      List(
        StructField("App", StringType, nullable = true),
        StructField("Category", ArrayType(StringType), nullable = true),
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


    val data = Seq(
      Row("App1", "Category1", "4.0", "1001", "2.3M", "1000+", "Paid", "$1.99", "Everyone", "Genre1;Genre2",
        "October 7, 2017", "1.0", "4.0"),
      Row("App1", "Category2", "4.0", "1000", "2.3M", "1000+", "Paid", "$1.99", "Everyone", "Genre1;Genre2",
        "October 7, 2017", "1.0", "4.0"),
      Row("App2", "Category3", "3.5", "500", "808k", "100+", "Paid", "$0.99", "Teen", "Genre3",
        "October 21, 2016", "1.1", "4.2")
    )
    val expectedData = Seq(
      Row("App1", Array("Category1","Category2"), 4.0, 1001L, 2.3, "1000+", "Paid", 1.791, "Everyone",
        Array("Genre1", "Genre2"), Date.valueOf("2017-10-07"), "1.0", "4.0"),
      Row("App2", Array("Category3"), 3.5, 500L, 0.808, "100+", "Paid", 0.891, "Teen",
        Array("Genre3"), Date.valueOf("2016-10-21"), "1.1", "4.2")
    )

    testDF = spark.createDataFrame(
      spark.sparkContext.parallelize(data),
      schema
    )

    expectedDF= spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      new_schema)
  }

  /**
   * Test DataFrame Transformation to Meet Part 3 Requirements
   *
   * Verifies that the DataFrame transformations applied in the `Part3` class satisfy the expected requirements.
   * Checks if the transformed DataFrame contains the necessary columns after the execution of transformations.
   */
  test("Test DataFrame transformation to meet Part 3 requirements") {
    val part3 = Part3(testDF)
    val resultDF = part3.execute(spark)

    // Test for the existence of columns after transformation
    assert(resultDF.columns.length === 13)
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
  }

  /**
   * Test Data Types
   *
   * Validates whether the data types of columns in the transformed DataFrame meet the expected types.
   */
  test("Test Data Types") {
    val part3 = Part3(testDF)
    val resultDF = part3.execute(spark)

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

  }

  /**
   * Test to Match Expected Output
   *
   * Verifies if the transformed DataFrame matches the expected DataFrame.
   */
  test("Test to match expected output "){
    val part3 = Part3(testDF)
    val resultDF = part3.execute(spark)

    assert(resultDF.exceptAll(expectedDF).count() == 0 && expectedDF.exceptAll(resultDF).count() == 0)

  }

  /**
   * Teardown
   */
  override def afterAll(): Unit = {
    spark.stop()
  }
}
