package example

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Created by lulei on 2018/3/20.
  */
object SkewedGroupByTest {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("GroupBy Test").master("local[*]")
      .getOrCreate()

    val numMappers = if (args.length > 0) args(0).toInt else 2
    var numKVPairs = if (args.length > 1) args(1).toInt else 1000
    val valSize = if (args.length > 2) args(2).toInt else 1000
    val numReducers = if (args.length > 3) args(3).toInt else numMappers

    val pairs1 = spark.sparkContext.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random

      // map output sizes linearly increase from the 1st to the last
      numKVPairs = (1.0 * (p + 1) / numMappers * numKVPairs).toInt

      val arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache()
    // Enforce that everything has been calculated and in cache
    pairs1.count()

    println(pairs1.groupByKey(numReducers).count())

    spark.stop()
  }
}
