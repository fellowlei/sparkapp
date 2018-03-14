package demo.df

import org.apache.spark.sql.{RelationalGroupedDataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by lulei on 2018/3/14.
  */
object DataFrameTest2 {
  def main(args:Array[String]): Unit ={
    val sparkConf = new SparkConf().setAppName( "Spark SQL DataFrame Operations").setMaster( "local[2]" )
    val sparkContext = new SparkContext(sparkConf)

    val sqlContext = new SQLContext(sparkContext);

    val url = "";
    val jdbcDF = sqlContext.read.format( "jdbc" ).options(
      Map( "url" -> url,
        "user" -> "root",
        "password" -> "root",
        "dbtable" -> "spark_sql_test" )).load()

    val joinDF1 = sqlContext.read.format( "jdbc" ).options(
      Map("url" -> url ,
        "user" -> "root",
        "password" -> "root",
        "dbtable" -> "spark_sql_join1" )).load()

    val joinDF2 = sqlContext.read.format( "jdbc" ).options(
      Map ( "url" -> url ,
        "user" -> "root",
        "password" -> "root",
        "dbtable" -> "spark_sql_join2" )).load()


    jdbcDF.show()

    jdbcDF.show(3)

    jdbcDF.show(true)
    jdbcDF.show(false)

    jdbcDF.collect();

    jdbcDF.collectAsList();

    jdbcDF.describe("name","age","phone").show()

    jdbcDF.first()

    jdbcDF.head(2)

    jdbcDF.take(2)

    jdbcDF.takeAsList(2)

    jdbcDF.where("name='mark' or age > 16")

    jdbcDF.filter("age > 16").show(2)

    jdbcDF.select("name","age").show(false)

    jdbcDF.select(jdbcDF("id"),jdbcDF("id") + 1).show(false)

    jdbcDF.selectExpr("id","name as NAME","round(age)").show()

    val idCol = jdbcDF.col("id");

    val idCol1 = jdbcDF.apply("id")
    val idCol2 = jdbcDF("id")

    jdbcDF.drop("id")

    jdbcDF.drop(jdbcDF("id"))

    jdbcDF.limit(3).show()

    jdbcDF.orderBy(- jdbcDF("id")).show()
    jdbcDF.orderBy(jdbcDF("id").desc).show()

    jdbcDF.orderBy("id").show()

    jdbcDF.groupBy("id")

    jdbcDF.groupBy(jdbcDF("id"))


    val group:RelationalGroupedDataset = jdbcDF.groupBy("id")
    group.max("age")
    group.min("age")
    group.mean("age")
    group.sum("age").show()
    group.count().show()


    jdbcDF.distinct().show()

    jdbcDF.dropDuplicates(Seq("name")).show()

    jdbcDF.agg("id" -> "max","age" -> "sum").show()

    jdbcDF.union(jdbcDF.limit(1))
    jdbcDF.unionAll(jdbcDF.limit(1))

    joinDF1.join(joinDF2).show()

    joinDF1.join(joinDF2,"id").show()

    joinDF1.join(joinDF2,Seq("id","name")).show()

    joinDF1.join(joinDF2,Seq("id","name"),"inner").show()

    joinDF1.join(joinDF2,joinDF1("id") === joinDF2("id")).show()

    joinDF1.join(joinDF2,joinDF1("id") === joinDF2("id"),"inner")

    jdbcDF.stat.freqItems(Seq("name"),0.3).show();

    jdbcDF.intersect(jdbcDF.limit(1)).show()

    jdbcDF.except(jdbcDF.limit(1)).show()

    jdbcDF.withColumnRenamed("id","idx")

    jdbcDF.withColumn("id2",jdbcDF("id")).show()






















































  }
}
