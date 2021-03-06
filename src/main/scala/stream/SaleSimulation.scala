package stream

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

/**
  * Created by lulei on 2017/12/19.
  * scala -cp sparkapp_2.11-1.0.jar stream.SaleSimulation /app/bigdata/log/abc.txt 9999 1000
  */
object SaleSimulation {
  def index(length:Int) = {
    import java.util.Random
    val rdm = new Random
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      System.err.println("Usage: <filename> <port> <millisecond>")
      System.exit(1)
    }

    val filename = args(0)
    val lines= Source.fromFile(filename).getLines().toList;
    val filerow = lines.length

    val listener  =new ServerSocket(args(1).toInt)
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(),true)
          while(true){
            Thread.sleep(args(2).toLong)
            val content = lines(index(filerow))
            print(content)
            out.write(content + "\n")
            out.flush()
          }
        }
      }.start()
    }
  }
}
