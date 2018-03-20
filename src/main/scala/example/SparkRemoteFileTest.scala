package example

import java.io.File

import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2018/3/20.
  */
object SparkRemoteFileTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkRemoteFileTest").master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    val path = "d:/log.txt"
    var rdd = sc.parallelize(Seq(1)).map(_ => {
      val localLocation = SparkFiles.get(path)
      println(s"${path} is stored at: $localLocation")
      new File(localLocation).isFile
    })

    val truthCheck = rdd.collect().head
    println(s"Mounting of ${path} was $truthCheck")
    spark.stop()
  }
}
