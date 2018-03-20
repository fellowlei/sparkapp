package example.sql

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by lulei on 2018/3/20.
  */
object RDDRelation {

  // One method for defining the schema of an RDD is to make a case class with the desired column
  // names and types.
  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RDDRelation").master("local[*]").config("spark.some.config.option", "some-value").getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._

    val df = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))

    // Any RDD containing case classes can be used to create a temporary view.  The schema of the
    // view is automatically inferred using scala reflection.
    df.createOrReplaceTempView("records")

    // Once tables have been registered, you can run SQL queries over them.
    println("Result of SELECT *:")
    spark.sql("SELECT * FROM records").collect().foreach(println)

    // Aggregation queries are also supported.
    val count = spark.sql("SELECT COUNT(*) FROM records").collect().head.getLong(0)
    println(s"COUNT(*): $count")

    // The results of SQL queries are themselves RDDs and support all normal RDD functions. The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSql = spark.sql("SELECT key, value FROM records WHERE key < 10")

    println("Result of RDD.map:")
    rddFromSql.rdd.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    df.where($"key" === 1).orderBy($"value".asc).select($"key").collect().foreach(println)


    // Write out an RDD as a parquet file with overwrite mode.
    df.write.mode(SaveMode.Overwrite).parquet("pair.parquet")

    // Read in parquet file.  Parquet files are self-describing so the schema is preserved.
    val parquetFile = spark.read.parquet("pair.parquet")


    // Queries can be run using the DSL on parquet files just like the original RDD.
    parquetFile.where($"key" === 1).select($"value".as("a")).collect().foreach(println)

    // These files can also be used to create a temporary view.
    parquetFile.createOrReplaceTempView("parquetFile")
    spark.sql("SELECT * FROM parquetFile").collect().foreach(println)

    spark.stop()


  }
}
