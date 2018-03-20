package demo.df

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2018/3/14.
  */
object DataFrameTest {

  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")
  val sqlContext = new SQLContext(sc);

  def dataframeDemo() = {
    val idAgeRDDRow = sc.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))
    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))

    val idAgeDF = sqlContext.createDataFrame(idAgeRDDRow,schema);
    // API不是面向对象的
    idAgeDF.filter(idAgeDF.col("age") > 25)
    // 不会报错, DataFrame不是编译时类型安全的
    idAgeDF.filter(idAgeDF.col("age") > "")
  }

  def rddDemo() = {
    // RDD Demo
      case class Person(id:Int,age:Int)
      val idAgeRDDPerson = sc.parallelize(Array(Person(1, 30), Person(2, 29), Person(3, 21)))
      idAgeRDDPerson.filter(_.age > 25)
      idAgeRDDPerson.foreach(println)
  }

  def dataSetDemo() = {
    val idAgeRDDRow = sc.parallelize(Array(Row(1, 30), Row(2, 29), Row(4, 21)))

    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType)))

    // 在2.0.0-preview中这行代码创建出的DataFrame, 其实是DataSet[Row]
    val idAgeDS = sqlContext.createDataFrame(idAgeRDDRow, schema)

//    sqlContext.createDataset(sc.parallelize(Array(1, 2, 3))).show()
  }

  def main(args: Array[String]): Unit = {


//    rddDemo()
//    dataframeDemo()
    dataSetDemo();
  }
}
