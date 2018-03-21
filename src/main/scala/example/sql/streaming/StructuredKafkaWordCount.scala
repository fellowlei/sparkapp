package example.sql.streaming

import java.util.UUID

import org.apache.spark.sql.SparkSession

/**
  * Created by lulei on 2018/3/21.
  */
object StructuredKafkaWordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("StructuredKafkaWordCount").getOrCreate()

    import spark.implicits._

    val bootstrapServers = "localhost:2181"
    val subscribeType = "assign"  // assign, subscribe, subscribePattern
    val topics = "mytopic"
    val checkpointLocation = "/tmp/temp-" + UUID.randomUUID().toString

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option(subscribeType, topics)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    // Generate running word count
    val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", checkpointLocation)
      .start()

    query.awaitTermination()
  }
}
