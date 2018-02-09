package demo

/**
  * Created by lulei on 2018/2/9.
  */
object ScalaBasic {

  def main(args: Array[String]): Unit = {
    //JOKE
    println(str())
    //6
    println(add(1,2,3))
    //12
    println(add2(3,9))
    //10
    println(add3(2))
    println("fac:"+fac(5))
    println("mulitply:"+mulitply(2)(6))
    println("m2:"+m2(2))
    println("t:"+t())
    println("testfunc02:"+testfunc02(t))
    println("d1:"+d1(5))
    println("testf1:"+testf1((a:Int,b:Int)=>{a*b}))
    println("testf1:"+testf1(add2))
    println("add4:"+add4(1,2,3))

    def sumInts = sum( x => x)
    println("sumInts:"+sumInts(1,3))
  }

  //直接赋值 返回出去
  def str(name : String = "JOKE")={name}

  //* 多个参数  遍历参数
  def add(sum : Int*)={
    var a = 0
    for(s <- sum){  a += s; }
    a
  }

  //先定义变量名 再定义类型
  def add2(a:Int,b:Int) =a+b

  //下划线在这里代表一个参数名  起名难问题
  def add3 = add2(_ : Int ,8)

  //f(n) = f(n)*f(n-1) 递归  需要给出返回的类型
  def fac(n:Int):Int = if(n<=0) 1 else n*fac(n-1);

  //    函数柯里化
  //就是说定义 把这个参数一个个独立开来写
  def mulitply(x:Int)(y:Int) = x*y
  //使用下划线
  def m2 = mulitply(2)_;

  // => 匿名函数声明方式
  val t = ()=>333//声明了一个函数对象付给了t

  // :后面是数据类型,c代表传进来的参数 传一个匿名函数进来 返回这个匿名函数
  def testfunc02(c : ()=>Int ) = { c() }
  //    匿名函数
  val d1 = (a:Int)=> a+100;


  //  匿名函数作为参数,其实就是参数名,后面跟上参数类型,然后是表达式
  //作用是传入表达式  改函数负责提供具体数据
  def testf1(callback : (Int,Int)=>Int )={
    callback(123,123);
  }

  //嵌套函数, def里面套一个def
  def add4(x:Int, y:Int ,z:Int) : Int = {
    def add2(i:Int, j:Int):Int = {
      i + j
    }
    add2(add2(x,y),z)
  }
  // 匿名函数 加递归
  def sum(f : Int => Int) : (Int , Int) => Int = {
    def sumF(a : Int , b : Int) : Int =
      if (a >b ) 0 else f(a) + sumF( a + 1 , b)
    sumF
  }

  //====================================================================

}
