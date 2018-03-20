package example.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2018/3/20.
  */
object SQLDataSourceExample {

  case class Person(name:String,age:Long)

  def runBasicDataSourceExample(spark: SparkSession):Unit = {
    val usersDF =  spark.read.load("users.parquet");
    usersDF.select("name","favorite_color").write.save("namesAndFavColors.parquet")


    val peopleDF = spark.read.format("json").load("people.json")
    peopleDF.select("name","age").write.format("parquest").save("namesAndAges.parquet")


    val peopleDFCSV = spark.read.format("csv").option("sep",";").option("inferSchema",true)
        .option("header","true").load("people.csv")

    val sqlDF = spark.sql("select * from parquet.`users.parquet`")
    peopleDF.write.bucketBy(42,"name").sortBy("age").saveAsTable("people_bucketed")


    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

    usersDF.write.partitionBy("favorite_color").bucketBy(42,"name").saveAsTable("users_partitioned_bucketed")

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")

  }

  def runBasicParquetExample(spark: SparkSession):Unit = {
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    val peopleDF = spark.read.json("people.json")
    peopleDF.write.parquet("people.json")

    val parquetFileDF = spark.read.parquet("people.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")

    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()

  }

  def runParquetSchemaMergingExample(spark: SparkSession) = {
    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i =>(i,i*i)).toDF("value","square")
    squaresDF.write.parquet("data/test_table/key=1")

    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i =>(i,i*i)).toDF("value","cube")
    cubesDF.write.parquet("data/test_table/key=2")

    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    mergedDF.printSchema()


  }

  def runJsonDatasetExample(spark: SparkSession) = {
    // Primitive types (Int, String, etc) and Product types (case classes) encoders are
    // supported by importing this when creating a Dataset.
    import spark.implicits._

    val path = "people.json"
    val peopleDF = spark.read.json(path)
    peopleDF.printSchema()

    peopleDF.createOrReplaceTempView("people")

    val teenagerNamesDF = spark.sql("select name from people wehre age between 13 and 19");
    teenagerNamesDF.show()

    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()

  }

  def runJdbcDatasetExample(spark: SparkSession) = {
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source

    val jdbcDF = spark.read.format("jdbc").option("url","jdbc:postgresql:dbserver").option("dbtable","schema.tablename")
      .option("user","username").option("password","password")
      .load()

    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")

    val jdbcDF2 = spark.read.jdbc("jdbc:postgresql:dbserver","schema.tablename",connectionProperties);


    // Specifying the custom data types of the read schema
    connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    val jdbcDF3 = spark.read
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Saving data to a JDBC source
    jdbcDF.write.format("jdbc").option("url","jdbc:postgresql:dbserver").option("dbtable","schema.tablename")
        .option("user","username").option("password","password").save()

    jdbcDF2.write.jdbc("jdbc:postgresql:dbserver","schema.tablename",connectionProperties)

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)


  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLDataSourceExample").master("local[*]")
        .config("spark.some.config.option", "some-value").getOrCreate()

    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)
    runParquetSchemaMergingExample(spark)
    runJsonDatasetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }
}
