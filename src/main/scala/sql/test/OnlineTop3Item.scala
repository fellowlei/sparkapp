package sql.test

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/21.
  * spark-submit --class "sql.test.OnlineTop3Item" --master local[*] sparkapp_2.11-1.0.jar
  */
object OnlineTop3Item {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("check")
    val clickLogStream = ssc.socketTextStream("localhost", 9999)

    val fmtClickLogStream = clickLogStream.filter(_.split(" ").length == 3).map(log => (log.split(" ")(2) + "_" + log.split(" ")(3), 1))
    val categoryClickLogStream = fmtClickLogStream.reduceByKeyAndWindow(_ + _, _ - _, Seconds(60), Seconds(20));
    categoryClickLogStream.foreachRDD(rdd => {
      if (rdd.isEmpty()) {

      } else {
        val categoryItemRow = rdd.map(reducedItem => {
          val category = reducedItem._1.split("_")(0)
          val item = reducedItem._1.split("_")(1)
          val click_count = reducedItem._2
          Row(category, item, click_count)
        })

        val structType = StructType(Array(
          new StructField("category", StringType, true),
          new StructField("item", StringType, true),
          new StructField("click_count", IntegerType, true)
        ))

        val sqlContext = new SQLContext(rdd.context);

        val categoryItemDf = sqlContext.createDataFrame(categoryItemRow, structType);
        categoryItemDf.registerTempTable("categoryItemTable")

        val resultDataFrame = sqlContext.sql("select category,item,click_count from " +
          "(select category,item,click_count,row_number() over(partition by category order by click_count desc) rank " +
          "from categoryItemTable) subquery " +
          "where rank <= 3")
        val resultRowRDD = resultDataFrame.rdd

        resultRowRDD.foreachPartition(records => {
          if (records.isEmpty) {
            // print("null")
          } else {
            //            insert to database
            //            val conn = ConnectionPool.getConnection()
            //            records.foreach(record =>{
            //              val sql = "insert into categorytop3(category,item,click_count) values("+
            //                record.getAs("category") + "," +
            //                record.getAs("item")+"," +
            //                record.getAs("click_count") +")"
            //
            //              val stmt = conn.createStatement()
            //              stmt.executeUpdate(sql)
            //            })
            //            ConnectionPool.returnConnection(connection)
          }
        })

        ssc.start()
        ssc.awaitTermination()
      }
    })
  }
}
