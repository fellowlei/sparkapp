package demo

/**
  * Created by lulei on 2018/2/9.
  */
object FileDemo {
  def main(args: Array[String]): Unit = {
      var path = FileDemo.getClass.getResource("/").getPath;
      path = path + "data/abc.txt";
      println(path)
  }
}
