package sql.demo

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2017/12/21.
  * spark-submit --class "sql.demo.CSVDemo" --master local[*] sparkapp_2.11-1.0.jar
  */
object CSVDemo {
  case class Employee(id:Int,name:String)

  def main(args: Array[String]): Unit = {
    // sparkContext
    val sparkSession = SparkSession.builder.master("local[*]")
      .appName("CSVDemo")
      .getOrCreate()



    val listEmployee = List(Employee(1, "iteblog"), Employee(2, "Jason"), Employee(3, "Abhi"))
    val empFrame = sparkSession.createDataFrame(listEmployee);
    empFrame.printSchema()
    empFrame.registerTempTable("emp")

    val emps = sparkSession.sql("select * from emp order by name desc")
    emps.show()



    val student = sparkSession.read.option("header","true").
      csv("data/student.csv")
    student.show()

    student.head(5).foreach(println)

    print(student.first())

    val emailFrame = student.select("email")

    emailFrame.show(3)

    val studentEmailFrame = student.select("studentName","email");
    studentEmailFrame.show(5)







  }
}
