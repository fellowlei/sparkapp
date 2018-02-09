package demo

import java.io.{File, PrintWriter}
import scala.io.Source

/**
  * Created by lulei on 2018/2/9.
  */
object InputDemo {
  def main(args: Array[String]): Unit = {
    in()
    file()
    url()
  }

  def file(): Unit ={
    val t = Source.fromFile("t.txt")
    for(i <- t.getLines()){
      println(i)
    }
    t.close()
  }

  def url(): Unit ={
    val t = Source.fromURL("http://www.baidu.com")
    t.foreach(print)
    t.close()
  }

  def in(): Unit = {
    val p = new PrintWriter(new File("t.txt"))
    p.println("new insert")
    p.close()
  }

}
