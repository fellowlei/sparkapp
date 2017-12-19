import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/18.
  * spark-submit --class "HelloWord" --master local[*] sparkapp_2.11-1.0.jar
  */
object HelloWord {
  def main(args: Array[String]): Unit = {
    val logFile = "/app/bigdata/log/test.log"
    val conf = new SparkConf().setAppName("Simple App").setMaster("local")
    val sc = new SparkContext(conf)
    val log = sc.textFile(logFile,2).cache()
    val item = log.filter(line => line.contains("item-pc")).count()
    val mssoa = log.filter(line => line.contains("mssoa")).count()
    println("source item:%s, source mssoa:%s".format(item,mssoa))
  }
}
