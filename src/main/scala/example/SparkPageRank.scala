package example

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2018/3/20.
  */
object SparkPageRank {

  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    showWarning()

    val spark = SparkSession.builder().appName("SparkPageRank").master("local[*]").getOrCreate()

    val iters = 10
    val lines = spark.read.text("d:/log.txt").rdd
    val links = lines.map{ s=>
      val parts = s.toString().split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    var ranks = links.mapValues(v => 1.0)

    for(i <- 1 to iters){
      val contribs = links.join(ranks).values.flatMap{
        case(urls,rank) =>
          val size = urls.size
          urls.map(url => (url,rank/size))
      }

    }

    val output = ranks.collect()
    output.foreach(tup => println(s"${tup._1} has rank: ${tup._2} ."))
    spark.stop()


  }
}
