package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/21.
  * spark-submit --class "sql.demo.CSVOutputDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object CSVOutputDemo {
  case class Student(id:Int,name:String,phone:String,email:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CSVOutputDemo").master("local[*]").getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    val student = spark.read.option("header","true").csv("data/student.csv")
    student.printSchema()



    val students = student.rdd.map(line =>Student(line(0).toString.toInt,line(1).toString,line(2).toString,line(3).toString)).toDF()
    students.printSchema()

    // 创建视图
    students.createOrReplaceTempView("student")

    val stu = spark.sql("select * from student")
    stu.show()

    // 保存数据到文件
    students.select("id","name","email").write.format("csv").save("data/student_out.csv")
    spark.stop()

  }
}
