package demo.d2

import org.apache.spark.sql.SparkSession


/**
  * Created by lulei on 2018/3/20.
  */
object SparkRowTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark SQL basic example").master("local[*]").config("spark.some.config.option", "some-value").getOrCreate()

    // Importing the SparkSession gives access to all the SQL functions and implicit conversions.
    import spark.implicits._

    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)] = List(
      (0, "male", 37, 10, "no", 3, 18, 7, 4),
      (0, "female", 27, 4, "no", 4, 14, 6, 4),
      (0, "female", 32, 15, "yes", 1, 12, 1, 4),
      (0, "male", 57, 15, "yes", 5, 18, 6, 5),
      (0, "male", 22, 0.75, "no", 2, 17, 6, 3),
      (0, "female", 32, 1.5, "no", 2, 17, 5, 5),
      (0, "female", 22, 0.75, "no", 2, 12, 1, 3),
      (0, "male", 57, 15, "yes", 2, 14, 4, 4),
      (0, "female", 32, 15, "yes", 4, 16, 1, 2),
      (0, "male", 22, 1.5, "no", 4, 14, 4, 5))


    val data = dataList.toDF("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")

    data.printSchema()

    // 展示前n条记录
    data.show(7)

    // 取前n条记录
    val data3=data.limit(5)

    // 过滤
    data.filter("age>50 and gender=='male'").show()

    // 数据框的所有列
    val columnArray=data.columns

    // 查询某些列的数据
    data.select("gender", "age", "yearsmarried", "children").show(3)


    // 操作指定的列,并排序
    // data.selectExpr("gender", "age+1","cast(age as bigint)").orderBy($"gender".desc, $"age".asc).show
    data.selectExpr("gender", "age+1 as age1","cast(age as bigint) as age2").sort($"gender".desc, $"age".asc).show


    val data4=data.selectExpr("gender", "age+1 as age1","cast(age as bigint) as age2").sort($"gender".desc, $"age".asc)

    // 查看物理执行计划
//    data4.explain()

    // 查看逻辑和物理执行计划
    data4.explain(extended=true)

    spark.stop()
  }
}
