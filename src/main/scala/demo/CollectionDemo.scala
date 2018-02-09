package demo

/**
  * Created by lulei on 2018/2/9.
  */
object CollectionDemo {
  def main(args: Array[String]): Unit = {
    var t = List(1,2,3,5,5)
    //下标从0
    //      println(t(2))


    //      map 个位置相加  函数编程
    //      println(t.map(a=> {
    //        //遍历每个元素
    //        print("***"+a);
    //        //每个值加2
    //        a+2})
    //        );

    //简洁版
    //      println(t.map(_+1));

    var t2 = t.+:("test");//添加元素
    //      println(6::t2);//一起打印 并没有放入进去
    //      println(t2);
    //      t2 = t::6::Nil;//组成新的List  t作为一个元素
    //      println(t2);

    //              t2.foreach(t=>print("---+++"+t))
    //      println("")
    //      println(t2);
    //遍历
    //      t2.foreach(println(_))

    //去重
    //      println(t.distinct)
    //从多少到多少的数字
    //      println(t.slice(0, 2))

    //      println("-*--*--*--*--*--*--*--*--*-")
    //遍历
    //      for(temp<-t2){
    //        print(temp)
    //      }
    //      println()

    //      println("-*--*--*--*--*--*--*--*--*-")
    //      for(i <- 0 to t2.length-1){
    //        print("--->i:"+i+" , ")
    //        print(t2(i))
    //      }


    //      println("-*--*--*--*--*--*--*--*--*-")
    //对这个集合 操作  从0 开始 累加上集合里面的值
    //0--1 1--2 3--3 6--5 11--5 16
    //3+1 4+2 6+3 9+5 14+5 19
    //      println(t./:(3)({
    //          (sum,num)=>
    //            print(sum+"+"+num+" ");
    //          sum+num
    //      }));

    //       1,2,3,5,5
    //集合里的值相加
    var vl = List(5,5,2,0)
    //      println(vl.reduce(_+_))
    //      println(vl.reduce((x,y)=> {
    //        println(x+","+y);
    //        x+y
    //        }
    //      ))


    //      println("-*--*--*--*--*--*--*--*--*-")
    //      println(t.foldLeft(10)((sum,num)=>{print(sum+"--"+num+" ");
    //          num+sum;
    //      }));

    //      println("-*--*--*--*--*--*--*--*--*-")
    //      println(t.map(v =>{println(v);v+2 }));
    //      println(t.map(v =>v+2));

    println("-*--*--*--*--*--*--*--*--*-")
    //      元组 定义以后不能改变 下标从1开始
    var tuple01 = (1,5,6,6);
    println(tuple01._1)
    println(tuple01._3)



  }
}
