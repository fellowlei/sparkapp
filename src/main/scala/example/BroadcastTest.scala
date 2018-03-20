package example

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2018/3/20.
  */
object BroadcastTest {
  def main(args: Array[String]): Unit = {
    val blockSize = 4096
    val spark = SparkSession.builder().appName("BroadcastTest") .config("spark.broadcast.blockSize", blockSize)
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext

    val slices = 2
    val num = 100000
    val arr1 = (0 until num).toArray

    for(i <- 0 until 3 ){
      println(s"Iteration $i")
      println("===========")
      val startTime = System.nanoTime();
      val bar1 = sc.broadcast(arr1)
      val observedSizes =sc.parallelize(1 to 10,slices).map(_ => bar1.value.length)
      observedSizes.collect().foreach(i => println(i))
      println("Iteration %d took %.0f milliseconds".format(i, (System.nanoTime - startTime) / 1E6))
    }
    spark.stop()
  }
}
