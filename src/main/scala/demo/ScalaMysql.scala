package demo

import java.sql.DriverManager


/**
  * Created by lulei on 2018/2/9.
  */
object ScalaMysql {
  def main(args: Array[String]): Unit = {
    insert();
  }


  def insert():Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://localhost:3306/test"

//    classOf[com.mysql.jdbc.Driver]
    Class.forName(driver)
    val conn = DriverManager.getConnection(url,"root","1");
    val prep = conn.prepareStatement("insert into user(id,name,age) values(?,?,?)");
    prep.setInt(1,1)
    prep.setString(2,"mark");
    prep.setInt(3,18);
    val result = prep.execute();
    println(result)
    conn.close()
  }


}
