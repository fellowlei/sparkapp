package example

import java.io.File

import org.apache.spark.sql.SparkSession

import scala.io.Source

/**
  * Created by lulei on 2018/3/20.
  */
object DFSReadWriteTest {
  private var dfsDirPath: String = ""

  private def readFile(filename: String): List[String] = {
    val lineIter: Iterator[String] = Source.fromFile(filename).getLines()
    val lineList: List[String] = lineIter.toList
    lineList
  }

  def runLocalWordCount(fileContents: List[String]) = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .mapValues(_.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    println("Performing local word count")
    val fileContents = readFile("d:/log.txt");
    val localWordCount = runLocalWordCount(fileContents)
    println("Creating SparkSession")

    val spark = SparkSession.builder().appName("DFS Read Write Test").master("local[*]").getOrCreate()

    val dfsFilename = s"$dfsDirPath/dfs_read_write_test"
    val fileRDD = spark.sparkContext.parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsFilename)

    println("Reading file from DFS and running Word Count")
    val readFileRDD =  spark.sparkContext.textFile(dfsFilename);

    val dfsWordCount = readFileRDD.flatMap(_.split(" "))
        .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w,1))
      .countByKey()
      .values.sum

    spark.stop()

  }


}
