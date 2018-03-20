package example

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.Random

/**
  * Created by lulei on 2018/3/20.
  */
object SparkTC {
  val numEdges = 200
  val numVertices = 100
  val rand = new Random(42)

  def generateGraph:Seq[(Int,Int)] = {
    val edges: mutable.Set[(Int,Int)] = mutable.Set.empty;
    while(edges.size < numEdges){
      val from = rand.nextInt(numVertices)
      val to = rand.nextInt(numVertices)
      if(from != to) edges.+=((from,to))
    }
    edges.toSeq
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkTC").master("local[2]").getOrCreate();
    var tc = spark.sparkContext.parallelize(generateGraph,2).cache();

    val edges = tc.map(x => (x._2,x._1))

    var oldCount = 0L
    val nextCount = tc.count()
    do{
      oldCount = nextCount
      tc = tc.union(tc.join(edges).map(x =>(x._2._2,x._2._1))).distinct().cache()
    }while(nextCount != oldCount)
    println(s"TC ahs ${tc.count()} edges.")
    spark.stop()

  }
}
