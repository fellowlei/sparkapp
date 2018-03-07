package demo.d2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fellowlei on 2018/3/7
  */
object SparkPairMap {
  val conf:SparkConf = new SparkConf().setAppName("spark pair map").setMaster("local[2]")
  val sc:SparkContext = new SparkContext(conf)

  /**
    * 构建Pair RDD
    */
  def createPairMap():Unit = {
    val rdd:RDD[(String,Int)] = sc.makeRDD(List(("k01",3),("k02",6),("k03",2),("k01",26)))
    val r:RDD[(String,Int)] = rdd.reduceByKey((x,y) => x + y)
    println("=========createPairMap=========")
    println(r.collect().mkString(","))// (k01,29),(k03,2),(k02,6)
    println("=========createPairMap=========")

    /*
     * 测试文件数据:
     * x01,1,4
             x02,11,1
             x01,3,9
             x01,2,6
       x02,18,12
       x03,7,9
     *
     * */
    val rddFile:RDD[(String,String)] = sc.textFile("file:///F:/sparkdata01.txt", 1).map { x => (x.split(",")(0),x.split(",")(1) + "," + x.split(",")(2)) }
    val rFile:RDD[String] = rddFile.keys
    println("=========createPairMap File=========")
    println(rFile.collect().mkString(","))// x01,x02,x01,x01,x02,x03
    println("=========createPairMap File=========")
  }

  /**
    * 关于Pair RDD的转化操作和行动操作
    */
  def pairMapRDD(path:String):Unit = {
    val rdd:RDD[(String,Int)] = sc.makeRDD(List(("k01",3),("k02",6),("k03",2),("k01",26)))
    val other:RDD[(String,Int)] = sc.parallelize(List(("k01",29)), 1)

    // 转化操作
    val rddReduce:RDD[(String,Int)] = rdd.reduceByKey((x,y) => x + y)
    println("====reduceByKey===:" + rddReduce.collect().mkString(","))// (k01,29),(k03,2),(k02,6)
    val rddGroup:RDD[(String,Iterable[Int])] = rdd.groupByKey()
    println("====groupByKey===:" + rddGroup.collect().mkString(","))// (k01,CompactBuffer(3, 26)),(k03,CompactBuffer(2)),(k02,CompactBuffer(6))
    val rddKeys:RDD[String] = rdd.keys
    println("====keys=====:" + rddKeys.collect().mkString(","))// k01,k02,k03,k01
    val rddVals:RDD[Int] = rdd.values
    println("======values===:" + rddVals.collect().mkString(","))// 3,6,2,26
    val rddSortAsc:RDD[(String,Int)] = rdd.sortByKey(true, 1)
    val rddSortDes:RDD[(String,Int)] = rdd.sortByKey(false, 1)
    println("====rddSortAsc=====:" + rddSortAsc.collect().mkString(","))// (k01,3),(k01,26),(k02,6),(k03,2)
    println("======rddSortDes=====:" + rddSortDes.collect().mkString(","))// (k03,2),(k02,6),(k01,3),(k01,26)
    val rddFmVal:RDD[(String,Int)] = rdd.flatMapValues { x => List(x + 10) }
    println("====flatMapValues===:" + rddFmVal.collect().mkString(","))// (k01,13),(k02,16),(k03,12),(k01,36)
    val rddMapVal:RDD[(String,Int)] = rdd.mapValues { x => x + 10 }
    println("====mapValues====:" + rddMapVal.collect().mkString(","))// (k01,13),(k02,16),(k03,12),(k01,36)
    val rddCombine:RDD[(String,(Int,Int))] = rdd.combineByKey(x => (x,1), (param:(Int,Int),x) => (param._1 + x,param._2 + 1), (p1:(Int,Int),p2:(Int,Int)) => (p1._1 + p2._1,p1._2 + p2._2))
    println("====combineByKey====:" + rddCombine.collect().mkString(","))//(k01,(29,2)),(k03,(2,1)),(k02,(6,1))
    val rddSubtract:RDD[(String,Int)] = rdd.subtractByKey(other);
    println("====subtractByKey====:" + rddSubtract.collect().mkString(","))// (k03,2),(k02,6)
    val rddJoin:RDD[(String,(Int,Int))] = rdd.join(other)
    println("=====rddJoin====:" + rddJoin.collect().mkString(","))// (k01,(3,29)),(k01,(26,29))
    val rddRight:RDD[(String,(Option[Int],Int))] = rdd.rightOuterJoin(other)
    println("====rightOuterJoin=====:" + rddRight.collect().mkString(","))// (k01,(Some(3),29)),(k01,(Some(26),29))
    val rddLeft:RDD[(String,(Int,Option[Int]))] = rdd.leftOuterJoin(other)
    println("=====rddLeft=====:" + rddLeft.collect().mkString(","))// (k01,(3,Some(29))),(k01,(26,Some(29))),(k03,(2,None)),(k02,(6,None))
    val rddCogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(other)
    println("=====cogroup=====:" + rddCogroup.collect().mkString(","))// (k01,(CompactBuffer(3, 26),CompactBuffer(29))),(k03,(CompactBuffer(2),CompactBuffer())),(k02,(CompactBuffer(6),CompactBuffer()))

    // 行动操作
    val resCountByKey = rdd.countByKey()
    println("=====countByKey=====:" + resCountByKey)// Map(k01 -> 2, k03 -> 1, k02 -> 1)
    val resColMap = rdd.collectAsMap()
    println("=====resColMap=====:" + resColMap)//Map(k02 -> 6, k01 -> 26, k03 -> 2)
    val resLookup = rdd.lookup("k01")
    println("====lookup===:" + resLookup) // WrappedArray(3, 26)
  }


  /**
    * 其他一些不常用的RDD操作
    */
  def otherRDDOperate(){
    val rdd:RDD[(String,Int)] = sc.makeRDD(List(("k01",3),("k02",6),("k03",2),("k01",26)))

    println("=====first=====:" + rdd.first())//(k01,3)
    val resTop = rdd.top(2).map(x => x._1 + ";" + x._2)
    println("=====top=====:" + resTop.mkString(","))// k03;2,k02;6
    val resTake = rdd.take(2).map(x => x._1 + ";" + x._2)
    println("=======take====:" + resTake.mkString(","))// k01;3,k02;6
    val resTakeSample = rdd.takeSample(false, 2).map(x => x._1 + ";" + x._2)
    println("=====takeSample====:" + resTakeSample.mkString(","))// k01;26,k03;2
    val resSample1 = rdd.sample(false, 0.25)
    val resSample2 = rdd.sample(false, 0.75)
    val resSample3 = rdd.sample(false, 0.5)
    println("=====sample======:" + resSample1.collect().mkString(","))// 无
    println("=====sample======:" + resSample2.collect().mkString(","))// (k01,3),(k02,6),(k01,26)
    println("=====sample======:" + resSample3.collect().mkString(","))// (k01,3),(k01,26)
  }


  def main(args: Array[String]): Unit = {
    createPairMap()
    pairMapRDD("file:///F:/sparkdata01.txt")
    otherRDDOperate()
  }


}
