package demo.d2

/**
  * Created by fellowlei on 2018/3/13
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.immutable.List

object ScalaTest {
  val conf: SparkConf = new SparkConf().setAppName("spark scala").setMaster("local[2]")
  val sc: SparkContext = new SparkContext(conf)

  def aggrFtnOne(par: ((Int, Int), Int)): (Int, Int) = {
    /*
       *aggregate的初始值为(0,0):
        ====aggrFtnOne Param===:((0,0),1)
                ====aggrFtnOne Param===:((1,1),2)
                ====aggrFtnOne Param===:((3,2),3)
                ====aggrFtnOne Param===:((6,3),4)
                ====aggrFtnOne Param===:((10,4),5)*/
    /*
       *aggregate的初始值为(1,1):
        ====aggrFtnOne Param===:((1,1),1)
        ====aggrFtnOne Param===:((2,2),2)
        ====aggrFtnOne Param===:((4,3),3)
        ====aggrFtnOne Param===:((7,4),4)
        ====aggrFtnOne Param===:((11,5),5)
       * */
    println("====aggrFtnOne Param===:" + par.toString())
    val ret: (Int, Int) = (par._1._1 + par._2, par._1._2 + 1)
    ret
  }

  def aggrFtnTwo(par: ((Int, Int), (Int, Int))): (Int, Int) = {
    /*aggregate的初始值为(0,0):::::((0,0),(15,5))*/
    /*aggregate的初始值为(1,1):::::((1,1),(16,6))*/
    println("====aggrFtnTwo Param===:" + par.toString())
    val ret: (Int, Int) = (par._1._1 + par._2._1, par._1._2 + par._2._2)
    ret
  }

  def foldFtn(par: (Int, Int)): Int = {
    /*fold初始值为0：
        =====foldFtn Param====:(0,1)
        =====foldFtn Param====:(1,2)
        =====foldFtn Param====:(3,3)
        =====foldFtn Param====:(6,4)
        =====foldFtn Param====:(10,5)
        =====foldFtn Param====:(0,15)
       * */
    /*
       * fold初始值为1:
        =====foldFtn Param====:(1,1)
        =====foldFtn Param====:(2,2)
        =====foldFtn Param====:(4,3)
        =====foldFtn Param====:(7,4)
        =====foldFtn Param====:(11,5)
        =====foldFtn Param====:(1,16)
       * */
    println("=====foldFtn Param====:" + par.toString())
    val ret: Int = par._1 + par._2
    ret
  }

  def reduceFtn(par:(Int,Int)):Int = {
    /*
     * ======reduceFtn Param=====:1:2
             ======reduceFtn Param=====:3:3
       ======reduceFtn Param=====:6:4
       ======reduceFtn Param=====:10:5
     */
    println("======reduceFtn Param=====:" + par._1 + ":" + par._2)
    par._1 + par._2
  }

  def sparkRDDHandle(): Unit = {
    val rddInt: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5), 1)

    val rddAggr1: (Int, Int) = rddInt.aggregate((0, 0))((x, y) => (x._1 + y, x._2 + 1), (x, y) => (x._1 + y._1, x._2 + y._2))
    println("====aggregate 1====:" + rddAggr1.toString()) // (15,5)

    val rddAggr2: (Int, Int) = rddInt.aggregate((0, 0))((x, y) => aggrFtnOne(x, y), (x, y) => aggrFtnTwo(x, y)) // 参数可以省略元组的括号
    println("====aggregate 2====:" + rddAggr2.toString()) // (15,5)

    val rddAggr3: (Int, Int) = rddInt.aggregate((1, 1))((x, y) => aggrFtnOne((x, y)), (x, y) => aggrFtnTwo((x, y))) // 参数使用元组的括号
    println("====aggregate 3====:" + rddAggr3.toString()) // (17,7)

    val rddAggr4: (Int, Int) = rddInt.aggregate((1, 0))((x, y) => (x._1 * y, x._2 + 1), (x, y) => (x._1 * y._1, x._2 + y._2))
    println("====aggregate 4====:" + rddAggr4.toString()) // (120,5)

    val rddFold1: Int = rddInt.fold(0)((x, y) => x + y)
    println("====fold 1====:" + rddFold1) // 15

    val rddFold2: Int = rddInt.fold(0)((x, y) => foldFtn(x, y)) // 参数可以省略元组的括号
    println("====fold 2=====:" + rddFold2) // 15

    val rddFold3: Int = rddInt.fold(1)((x, y) => foldFtn((x, y))) // 参数使用元组的括号
    println("====fold 3====:" + rddFold3) // 17

    val rddReduce1:Int = rddInt.reduce((x,y) => x + y)
    println("====rddReduce 1====:" + rddReduce1)// 15

    val rddReduce2:Int = rddInt.reduce((x,y) => reduceFtn(x,y))
    println("====rddReduce 2====:" + rddReduce2)// 15

  }

  def combineFtnOne(par:Int):(Int,Int) = {
    /*
     * ====combineFtnOne Param====:2
       ====combineFtnOne Param====:5
       ====combineFtnOne Param====:8
       ====combineFtnOne Param====:3
     */
    println("====combineFtnOne Param====:" + par)
    val ret:(Int,Int) = (par,1)
    ret
  }

  def combineFtnTwo(par:((Int,Int),Int)):(Int,Int) = {
    /*
      ====combineFtnTwo Param====:((2,1),12)
      ====combineFtnTwo Param====:((8,1),9)
     * */
    println("====combineFtnTwo Param====:" + par.toString())
    val ret:(Int,Int) = (par._1._1 + par._2,par._1._2 + 1)
    ret
  }

  def combineFtnThree(par:((Int,Int),(Int,Int))):(Int,Int) = {
    /*
     * 无结果打印
     */
    println("@@@@@@@@@@@@@@@@@@")
    println("====combineFtnThree Param===:" + par.toString())
    val ret:(Int,Int) = (par._1._1 + par._2._1,par._1._2 + par._2._2)
    ret
  }

  def flatMapRange(par:Int):Range = {
    par to 6
  }

  def flatMapList(par:Int):List[Int] = {
    List(par + 1000)
  }

  def flatMapSeq(par:Int):Seq[Int] = {
    Seq(par + 6000)
  }

  def sparkPairRDD(): Unit = {
    val rddPair: RDD[(String, Int)] = sc.parallelize(List(("x01", 2), ("x02", 5), ("x03", 8), ("x04", 3), ("x01", 12), ("x03", 9)), 1)

    /* def combineByKey[C](createCombiner: Int => C, mergeValue: (C, Int) => C, mergeCombiners: (C, C) => C): RDD[(String, C)] */
    val rddCombine1:RDD[(String,(Int,Int))] = rddPair.combineByKey(x => (x, 1), (com: (Int, Int), x) => (com._1 + x, com._2 + 1), (com1: (Int, Int), com2: (Int, Int)) => (com1._1 + com2._1, com1._2 + com2._2))
    println("====combineByKey 1====:" + rddCombine1.collect().mkString(",")) // (x02,(5,1)),(x03,(17,2)),(x01,(14,2)),(x04,(3,1))

    val rddCombine2:RDD[(String,(Int,Int))] = rddPair.combineByKey(x => combineFtnOne(x), (com: (Int, Int), x) => combineFtnTwo(com,x), (com1: (Int, Int), com2: (Int, Int)) => combineFtnThree(com1,com2))
    println("=====combineByKey 2====:" + rddCombine2.collect().mkString(",")) // (x02,(5,1)),(x03,(17,2)),(x01,(14,2)),(x04,(3,1))


    val rddKeys:RDD[String] = rddPair.keys
    /*结果:x01,x02,x03,x04,x01,x03  注意调用keys方法时候不能加上括号，否则会报错*/
    println("====keys====:" + rddKeys.collect().mkString(","))

    val rddVals:RDD[Int] = rddPair.values
    /*结果：2,5,8,3,12,9  注意调用values方法时候不能加上括号，否则会报错*/
    println("=====values=====:" + rddVals.collect().mkString(","))

    val rddFlatMapVals1:RDD[(String,Int)] = rddPair.flatMapValues { x => x to (6) }
    /* 结果：(x01,2),(x01,3),(x01,4),(x01,5),(x01,6),(x02,5),(x02,6),(x04,3),(x04,4),(x04,5),(x04,6) */
    println("====flatMapValues 1====:" + rddFlatMapVals1.collect().mkString(","))
    val rddFlatMapVals2:RDD[(String,Int)] = rddPair.flatMapValues { x => flatMapRange(x) }
    /* 结果：(x01,2),(x01,3),(x01,4),(x01,5),(x01,6),(x02,5),(x02,6),(x04,3),(x04,4),(x04,5),(x04,6) */
    println("====flatMapValues 2====:" + rddFlatMapVals2.collect().mkString(","))
    val rddFlatMapVals3:RDD[(String,Int)] = rddPair.flatMapValues { x => flatMapList(x) }
    /* 结果：(x01,1002),(x02,1005),(x03,1008),(x04,1003),(x01,1012),(x03,1009) */
    println("====flatMapValues 3====:" + rddFlatMapVals3.collect().mkString(","))
    val rddFlatMapVals4:RDD[(String,Int)] = rddPair.flatMapValues { x => flatMapSeq(x) }
    /* 结果：(x01,6002),(x02,6005),(x03,6008),(x04,6003),(x01,6012),(x03,6009) */
    println("====flatMapValues 4====:" + rddFlatMapVals4.collect().mkString(","))

    val rddFlatMap1:RDD[(String,Int)] = rddPair.flatMap(x => List((x._1,x._2 + 3000)))
    // 结果：(x01,3002),(x02,3005),(x03,3008),(x04,3003),(x01,3012),(x03,3009)
    println("=====flatMap 1======:" + rddFlatMap1.collect().mkString(","))
    val rddFlatMap2:RDD[Int] = rddPair.flatMap(x => List(x._2 + 8000))
    // 结果:8002,8005,8008,8003,8012,8009
    println("=====flatMap 2======:" + rddFlatMap2.collect().mkString(","))
    val rddFlatMap3:RDD[String] = rddPair.flatMap(x => List(x._1 + "@!@" + x._2))
    // 结果：x01@!@2,x02@!@5,x03@!@8,x04@!@3,x01@!@12,x03@!@9
    println("=====flatMap 3======:" + rddFlatMap3.collect().mkString(","))
  }

  def optionSome():Unit = {
    /*
     * =======option for 1=========
      0:3
      2:8
      3:11
      =======option for 1=========
      =======option for 2=========
      0:3
      1:None
      2:8
      3:11
      =======option for 2=========
     */
    val list:List[Option[Int]] = List(Some(3),None,Some(8),Some(11))
    println("=======option for 1=========")
    for (i <- 0 until list.size){
      if (!list(i).isEmpty){
        println(i + ":" + list(i).get)
      }
    }
    println("=======option for 1=========")
    println("=======option for 2=========")
    for (j <- 0 until list.size){
      val res = list(j) match {
        case None => println(j + ":None")
        case _ => println(j + ":" + list(j).get)
      }
    }
    println("=======option for 2=========")
  }

  def pairRDDJoinGroup():Unit = {
    val rdd:RDD[(String,Int)] = sc.makeRDD(List(("x01",2),("x02",5),("x03",9),("x03",21),("x04",76)))
    val other:RDD[(String,Int)] = sc.makeRDD(List(("x01",4),("x02",6),("x03",11)))

    val rddRight:RDD[(String,(Option[Int],Int))] = rdd.rightOuterJoin(other)
    /* 结果：(x02,(Some(5),6)),(x03,(Some(9),11)),(x03,(Some(21),11)),(x01,(Some(2),4)) */
    println("====rightOuterJoin====:" + rddRight.collect().mkString(","))

    val rddLeft:RDD[(String,(Int,Option[Int]))] = rdd.leftOuterJoin(other)
    /* 结果： (x02,(5,Some(6))),(x04,(76,None)),(x03,(9,Some(11))),(x03,(21,Some(11))),(x01,(2,Some(4))) */
    println("====leftOuterJoin====:" + rddLeft.collect().mkString(","))
    val rddSome = rddLeft.filter(x => x._2._2.isEmpty == false)// 过滤掉None的记录
    /* 结果: (x02,(5,Some(6))),(x03,(9,Some(11))),(x03,(21,Some(11))),(x01,(2,Some(4)))*/
    println("====rddSome===:" + rddSome.collect().mkString(","))

    val rddCogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd.cogroup(other)
    /* 结果: (x02,(CompactBuffer(5),CompactBuffer(6))),(x04,(CompactBuffer(76),CompactBuffer())),(x03,(CompactBuffer(9, 21),CompactBuffer(11))),(x01,(CompactBuffer(2),CompactBuffer(4)))*/
    println("===cogroup====:" + rddCogroup.collect().mkString(","))
  }

  def scalaBasic(){
    val its:Iterable[Int] = Iterable(1,2,3,4,5)
    its.foreach { x => print(x + ",") }// 1,2,3,4,5,

    val tuple2Param1:Tuple2[String,Int] = Tuple2("x01",12)// 标准定义二元组
    val tuple2Param2:(String,Int) = ("x02",29)// 字面量定义二元组

    /* 结果: x01:12*/
    println("====tuple2Param1====:" + tuple2Param1._1 + ":" + tuple2Param1._2)
    /* 结果: x02:29 */
    println("====tuple2Param2====:" + tuple2Param2._1 + ":" + tuple2Param2._2)

    val tuple6Param1:Tuple6[String,Int,Int,Int,Int,String] = Tuple6("xx01",1,2,3,4,"x1x")// 标准定义6元组
    val tuple6Param2:(String,Int,Int,Int,Int,String) = ("xx02",1,2,3,4,"x2x")// 字面量定义6元组

    /* 结果: xx01:1:2:3:4:x1x */
    println("====tuple6Param1====:" + tuple6Param1._1 + ":" + tuple6Param1._2 + ":" + tuple6Param1._3 + ":" + tuple6Param1._4 + ":" + tuple6Param1._5 + ":" + tuple6Param1._6)
    /* 结果: xx02:1:2:3:4:x2x */
    println("====tuple6Param2====:" + tuple6Param2._1 + ":" + tuple6Param2._2 + ":" + tuple6Param2._3 + ":" + tuple6Param2._4 + ":" + tuple6Param2._5 + ":" + tuple6Param2._6)

    val list:List[Int] = List(1,2,3,4,5,6)
    val len:Int = list.size - 1
    val r:Range = 0 to len

    for (ind <- r){
      print(list(ind) + ";")// 1;2;3;4;5;6;
    }
    println("")
    for (ind <- 0 to len){
      print(list(ind) + ";")// 1;2;3;4;5;6;
    }
    println("")
  }

  def main(args: Array[String]): Unit = {
    scalaBasic()
    optionSome()

    sparkRDDHandle()
    sparkPairRDD()
    pairRDDJoinGroup()


  }
}
