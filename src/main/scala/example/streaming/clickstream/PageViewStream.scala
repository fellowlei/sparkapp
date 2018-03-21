package example.streaming.clickstream

import example.streaming.clickstream.PageViewGenerator.PageView
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by lulei on 2018/3/21.
  */
object PageViewStream {
  def main(args: Array[String]): Unit = {
    // metric must be one of pageCounts, slidingPageCounts, errorRatePerZipCode, activeUserCount, popularUsersSeen
    val metric = "pageCounts"
    val host = "localhost"
    val port = 8888

    // Create the context
    val sparkConf = new SparkConf().setAppName("PageViewStream").setMaster("local[*]");
    val ssc = new StreamingContext(sparkConf,Seconds(1))

    // Create a ReceiverInputDStream on target host:port and convert each line to a PageView
    val pageViews = ssc.socketTextStream(host, port)
      .flatMap(_.split("\n"))
      .map(PageView.fromString(_))


    // Return a count of views per URL seen in each batch
    val pageCounts = pageViews.map(view => view.url).countByValue()

    // Return a sliding window of page views per URL in the last ten seconds
    val slidingPageCounts = pageViews.map(view => view.url)
      .countByValueAndWindow(Seconds(10), Seconds(2))

    // Return the rate of error pages (a non 200 status) in each zip code over the last 30 seconds
    val statusesPerZipCode = pageViews.window(Seconds(30), Seconds(2))
      .map(view => ((view.zipCode, view.status)))
      .groupByKey()

    val errorRatePerZipCode = statusesPerZipCode.map{
      case(zip, statuses) =>
        val normalCount = statuses.count(_ == 200)
        val errorCount = statuses.size - normalCount
        val errorRatio = errorCount.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(zip, errorRatio)
        } else {
          "%s: %s".format(zip, errorRatio)
        }
    }

    // Return the number unique users in last 15 seconds
    val activeUserCount = pageViews.window(Seconds(15), Seconds(2))
      .map(view => (view.userID, 1))
      .groupByKey()
      .count()
      .map("Unique active users: " + _)

    // An external dataset we want to join to this stream
    val userList = ssc.sparkContext.parallelize(Seq(
      1 -> "Patrick Wendell",
      2 -> "Reynold Xin",
      3 -> "Matei Zaharia"))

    metric match {
      case "pageCounts" => pageCounts.print()
      case "slidingPageCounts" => slidingPageCounts.print()
      case "errorRatePerZipCode" => errorRatePerZipCode.print()
      case "activeUserCount" => activeUserCount.print()
      case "popularUsersSeen" =>
        // Look for users in our existing dataset and print it out if we have a match
        pageViews.map(view => (view.userID, 1))
          .foreachRDD((rdd, time) => rdd.join(userList)
            .map(_._2._2)
            .take(10)
            .foreach(u => println(s"Saw user $u at time $time")))
      case _ => println(s"Invalid metric entered: $metric")
    }


    ssc.start()
    ssc.awaitTermination()


  }
}
