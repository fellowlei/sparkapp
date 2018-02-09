package demo

import java.io.File

import scala.io.Source

/**
  * Created by lulei on 2018/2/9.
  */
object FileDemo {
  def main(args: Array[String]): Unit = {
      var path = FileDemo.getClass.getResource("/").getPath;
      path = path + "data/abc.txt";
      println(path)


    val source = Source.fromFile(path);
//    val lines =source.getLines();
//    for(line <- lines){
//      println(line)
//    }
//    source.close()

//    val lines1 = source.getLines().toArray;
//    val lines2 = source.getLines().toBuffer;
//    println(lines1)
//    println(lines2)

//    val lines = source.mkString;
//    println(lines)

//    for(c <- source){
//      print(c)
//    }

//    val iter = source.buffered;
//    while(iter.hasNext){
//      if(iter.next().equals("k")){
//        print("#")
//      }else{
//        print("-")
//      }
//    }
//    val iter = source.mkString.split("\\s+")
//    for(w <- iter){
//      println(w)
//    }


//    val s1 = Source.fromURL("http://www.baidu.com");
//    val s2 = Source.fromString("hello");
//    val lines = s1.getLines();
//    for(line <- lines){
//      println(line)
//    }
//    println(s1)
//    println(s2)
//    val s3 = Source.stdin;


  }

  def listFile(): Unit ={
    for(d <- subDir(new File("F:\\gitcodetest\\sparkapp\\src\\main\\java"))){
      println(d)
    }
  }

  def subDir(dir:File):Iterator[File] ={
    val dirs = dir.listFiles().filter(_.isDirectory);
    val files = dir.listFiles().filter(_.isFile);
    files.toIterator ++ dirs.toIterator.flatMap(subDir(_))
  }
}
