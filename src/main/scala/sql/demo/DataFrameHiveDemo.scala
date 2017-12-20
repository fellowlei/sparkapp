package sql.demo

import java.io.File

import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameHiveDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameHiveDemo {
  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val spark = SparkSession.builder().appName("Spark Hive Example").master("local[*]")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }

    stringsDS.show()

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()


  }
}
