package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/20.
  * spark-submit --class "sql.demo.DataFrameDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object DataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameDemo").master("local[*]").getOrCreate();

    // For implicit conversions like converting RDDs to DataFrames
    val df = spark.read.json("data/people.json")

    // This import is needed to use the $-notation
    import spark.implicits._
    // Print the schema in a tree format
    df.printSchema()

    // Select only the "name" column
    df.select("name").show()

    // Select everybody, but increment the age by 1
    df.select($"name", $"age" + 1).show()

    // Select people older than 21
    df.filter($"age" > 21).show()

    // Count people by age
    df.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    df.createOrReplaceTempView("people")

    val sqlDF = spark.sql("select * from people")
    sqlDF.show()

    case class Person(name:String,age:Long)

    // Encoders are created for case classes
    val caseClassDS = Seq(Person("Andy", 32)).toDS()
    caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
    val path = "data/people.json"
    val peopleDs = spark.read.json(path).as[Person]
    peopleDs.show()

  }
}
