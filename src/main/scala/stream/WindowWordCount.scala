package stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "stream.WindowWordCount" --master local[*] gitcodesbt_2.11-1.0.jar 127.0.0.1 9999 30 10
  */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //创建StreamingContext
    val ssc = new StreamingContext(sc,Seconds(5))
    ssc.checkpoint(".")

    //获取数据
    val lines = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_ONLY_SER)
    val words = lines.flatMap(_.split(" "))

    //windows操作
    val wordCounts = words.map(x =>(x,1)).reduceByKeyAndWindow((a:Int,b:Int) =>(a + b),Seconds(args(2).toInt),Seconds(args(3).toInt))
    //第二个参数是 windows的窗口时间间隔，比如是 监听间隔的 倍数，上面是 5秒，这里必须是5的倍数。eg :30
    //第三个参数是 windows的滑动时间间隔，也必须是监听间隔的倍数。eg :10
    //那么这里的作用是， 每隔10秒钟，对前30秒的数据， 进行一次处理，这里的处理就是 word count。
    //val wordCounts = words.map(x => (x , 1)).reduceByKeyAndWindow(_+_, _-_,Seconds(args(2).toInt), Seconds(args(3).toInt))
    //这个是优化方法， 即加上上一次的结果，减去 上一次存在又不在这一次的数据块的部分。

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
