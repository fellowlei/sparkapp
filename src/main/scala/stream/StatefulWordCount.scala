package stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "stream.StatefulWordCount" --master local[*] gitcodesbt_2.11-1.0.jar 127.0.0.1 9999
  */
object StatefulWordCount {
  def main(args: Array[String]): Unit = {
      val updateFun = (values:Seq[Int],state:Option[Int]) => { //StateFul需要定义的处理函数，第一个参数是本次进来的值，第二个是过去处理后保存的值
        val currentCount = values.foldLeft(0)(_ + _)// 求和
        val previousCount = state.getOrElse(0) // 如果过去没有 即取0
        Some(currentCount + previousCount) // 求和
      }

    val conf = new SparkConf().setAppName("StatefulWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc,Seconds(5))// 创建StreamingContext
    ssc.checkpoint(".")// 因为是有状态的，需要保存之前的信息，所以这里设定了 checkpoint的目录，以防断电后内存数据丢失。
    //这里因为没有设置checkpoint的时间间隔，所以会发现每一次数据块过来 即切分一次，产生一个 .checkpoint 文件

    val lines = ssc.socketTextStream(args(0),args(1).toInt)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x,1))

    //使用updateStateByKey来更新状态
    val stateDstream = wordCounts.updateStateByKey(updateFun);//调用 处理函数 updateFunc
    stateDstream.print()

    ssc.start()
    ssc.awaitTermination()



  }
}
