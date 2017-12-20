package sql

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2017/12/19.
  * spark-submit --class "sql.MovieDataStatistic" --master local[*] sparkapp_2.11-1.0.jar
  */
object MovieDataStatistic {
  case class Rating(userId:Int,movieId:Int,rating:Double)

  case class Movie(id:Int,tile:String,releaseDate:String)

  case class User(id:Int,age:Int,gender:String)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MovieDataStatistic").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._
    val ratingsDF:DataFrame = sc.textFile("/app/bigdata/log/rating.data").map(x =>x.split("::").map(line =>Rating(line(0).toInt,line(1).toInt,line(2).toDouble))).toDF();
    ratingsDF.registerTempTable("ratings")

    //查看338评分记录条数
    println("sql for 338 rateing info is : ")
    sqlContext.sql("select * from ratings where userId=338").show()
    println("dataframe 338 rateing info is : ")
    ratingsDF.filter(ratingsDF("userId").equalTo(338)).show()

    val userDf = sc.textFile("/app/bigdata/log/user.data").map(x =>x.split("[|]")).map(line => User(line(0).toInt,line(1).toInt,line(2))).toDF()
    userDf.registerTempTable("users")
    sqlContext.sql("select * from users where id = 338").show()
    userDf.filter(userDf("id").equalTo(338)).show()

    val movieDf = sc.textFile("/movie.data").map(x => x.split("::")).map(line => Movie(line(0).toInt,line(1),line(2))).toDF();
    movieDf.registerTempTable("movies")
    movieDf.collect()
    sqlContext.sql("select * from movies where id=1").show()
    movieDf.filter(movieDf("id").equalTo(1)).show()

    sqlContext.sql("select r.userId,m.title,r.rating from movies m inner join ratings r " +
                    "on m.id = r.movieId and r.userId = 338 order by r.rating desc").show()
    val resultDf = movieDf.join(ratingsDF.filter(ratingsDF("userId").equalTo(338)), movieDf("id").equalTo(ratingsDF("movieId")))
      .sort(ratingsDF("rating").desc).select("userId","title","rating")

    resultDf.collect().foreach(println)

    import org.apache.spark.sql.functions._
//    将结果保存至json格式
//    val saveOptions = Map("header" -> "true", "path" -> "/data/rat_movie.csv")
//    resultDf.write.format("json").mode(SaveMode.Overwrite).options(saveOptions).save()

    // 评论电影最多的用户id
    sqlContext.sql("select userId,count(*) as count from ratings group by userId order by count desc ").show(1)
    val userIdCountDf = ratingsDF.groupBy("userId").count()
    userIdCountDf.join(userIdCountDf.agg(max("count").alias("max_count"))
      ,$"count".equalTo($"max_Count")).select("userId").show(1)

    // 被用户评论最多的电影id、title
    val movieIdGroupDF = ratingsDF.groupBy("movieId").count();
    val movieCountDf = movieIdGroupDF.join(movieIdGroupDF.agg(max("count").alias("max_count")))
      .filter($"count".equalTo($"max_count"))
    //星球大战是被用户评论最多的电影
    movieCountDf.join(movieDf).filter($"movieId".equalTo($"id")).select("movieId","title","releaseDate").show()

    // 评论电影年龄最小者、最大者
    // 年龄最大的73岁，最小的7岁
    ratingsDF.join(userDf,ratingsDF("userId").equalTo(userDf("id")))
      .agg(min($"age").alias("min_age"),max($"age").alias("max_age"))
        .join(userDf,$"age".isin($"min_age",$"max_age"))
        .select("id","age","gender").show(2)

    // 25至30岁的用户欢迎的电影
    userDf.filter($"age".between(25,30)).join(ratingsDF,$"id".equalTo($"userid"))
        .select("userId","movieId","rating").join(movieDf,$"rating".equalTo(5))
        .select("movieId","title").show(10)

    // 最受用户喜爱的电影
    ratingsDF.groupBy("movieId").agg(avg("rating").alias("avg_rate"))
        .sort($"avg_rate".desc).limit(10)
        .join(movieDf,$"movieId".equalTo($"id"))
        .select("title").show(false)




  }
}
