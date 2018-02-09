package demo

import java.util

/**
  * Created by lulei on 2018/2/9.
  */
object ListDemo {
  def main(args: Array[String]): Unit = {
//    test()
//    test2()
//    test3()
//    println(testmatch(3));
//    testList()
    testMap();
  }

  def test(): Unit ={
    for(i <- 1 to 10){
      println(i)
    }
  }

  def test2(): Unit ={
    for(i <- 1 until 10){
      println(i)
    }
  }

  //直接加入表達式在循环里面
  def test3(): Unit ={
    for(i <- 0 to 100 if (i % 2 ) == 1; if(i % 5) > 3){
      println("i:" + i)
    }
  }

  def testmatch(n:Int): Unit ={
    n match {
      case  1=> {println("111");n;}
      case 2=> println("2222");n;
      case _=> println("other");"test";
    }
  }

  def testMap(): Unit ={
    //    _ 通配符  =>匿名函数   <- for便利符号

    // mutable
    // immutable
    var m1 = Map[String, Int](("a", 1), ("b", 2));

    println(m1("a"));
    //往map中加入元素
    m1 += ("c" -> 3);
    println(m1)
    //a 是一个元祖
    m1.foreach(a => {
      println(a + " " + a._1 + " " + a._2)
    });
    //遍历map
    //    m1.keys.foreach(b => println(m1(b)));
    //获取到map 的键值
    m1.keys.foreach(b => print(b));
    println()
    m1.values.foreach(a => print(a));
    println()
    println(m1)

  }

  def testList(): Unit ={
    var arr = new util.ArrayList[Any]();
    arr.add("abc")
    arr.add(123)
    println(arr)
    println(arr.get(0))
    println("iter")

    val it = arr.iterator();
    while(it.hasNext){
      println(it.next())
    }
  }


}
