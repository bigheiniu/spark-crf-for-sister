import org.apache.spark.{SparkConf, SparkContext, rdd}

import util.control.Breaks._
import java.io.PrintWriter
import java.io.File

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.I
import org.apache.spark

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object test1 {

  class Node (var xc:Int = 0, var yc:Int = 0, var mc:Double=0.0){
    var x: Int = xc
    var y: Int = yc
    var m: Double = mc
  }
  val erjie = new ArrayBuffer[Node]()

  var n = 0
  var matrixLAPP = new ArrayBuffer[Map[Int, ArrayBuffer[Int]]]()
  var length = 0

  //八种情况
  def judge(X:ArrayBuffer[Int], Y:ArrayBuffer[Int], chosen:Int, j:Int): Boolean = {
    var result: Boolean = true
    val a = if (X(j) == 1) true else false
    val b = if (Y(j) == 1) true else false
    if (chosen == 1) result = a && b
    if (chosen == 2) result = !(a && b)
    if (chosen == 3) result = a || b
    if (chosen == 4) result = !(a || b)
    if (chosen == 51) result = a && (!b)
    if (chosen == 52) result = (!a) && b
    if (chosen == 61) result = a || (!b)
    if (chosen == 62) result = (!a) || b
    if (chosen == 7) result = !(a == b)
    if (chosen == 8) result = a == b
    return result
  }

  //一阶信息熵
  def H(X: ArrayBuffer[Int]): Double = {
    if (X==null || X.length <= 0) return 0.0
    val cnt = new Array[Int](2)
    for (j <- 0 until X.length) {
      if (X(j) == 0) cnt(0) += 1
      else if (X(j) == 1) cnt(1) += 1
    }
    val p = new Array[Double](2)
    for (i <- 0 until 2) {
      if (cnt(i) == 0) p(i) = 0.0
      else p(i) = cnt(i) * 1.0 / X.length
    }

    var ans:Double = 0.0
    for (i <- 0 until 2) {
      breakable {
        if ((p(i) - 0.0).abs < 1e-6) break()
        ans += (p(i) * Math.log(p(i)))
      }
    }
    return -ans
  }

  //二阶信息熵
  def H(X:ArrayBuffer[Int], Y:ArrayBuffer[Int]): Double = {
    if (X==null || Y==null || X.length <= 0 || Y.length <= 0) return 0.0
    val cnt = new Array[Int](4)
    for (j <- 0 until X.length) {
      val a = if (X(j) == 1) true else false
      val b = if (Y(j) == 1) true else false
      if (!a && !b) {
        cnt(0) += 1
      }
      else if (!a && b) {
        cnt(1) += 1
      }
      else if (a && !b) {
        cnt(2) += 1
      }
      else if (a && b) {
        cnt(3) += 1
      }
    }

    val p = new Array[Double](4)
    for (i <- 0 until 4) {
      if (cnt(i) == 0) p(i) = 0.0
      else p(i) = cnt(i) * 1.0 / X.length
    }

    var ans:Double = 0.0
    for (i <- 0 until 4) {
      breakable {
        if ((p(i) - 0.0).abs < 1e-6 || p(i) < 0) break()
        ans += (p(i) * Math.log(p(i)))
      }
    }
    return -ans
  }

  //H(f(x, y))信息熵
  def H(X:ArrayBuffer[Int], Y:ArrayBuffer[Int], chosen:Int): Double = {
    val cnt = new Array[Int](2)
    for (j <- 0 until X.length) {
      if (!judge(X, Y, chosen, j)) {
        cnt(0) += 1
      }
      else if (judge(X, Y, chosen, j)) {
        cnt(1) += 1
      }
    }
    val p = new Array[Double](2)
    for (i <- 0 until 2) {
      if (cnt(i) == 0) p(i) = 0.0
      else p(i) = cnt(i) * 1.0 / X.length
    }

    var ans:Double = 0.0
    for (i <- 0 until 2) {
      breakable {
        if ((p(i) - 0.0).abs < 1e-6) break()
        ans += (p(i) * Math.log(p(i)))
      }
    }
    return -ans
  }

  // H(Z | (X, Y))
  def H(Z:ArrayBuffer[Int], X:ArrayBuffer[Int], Y:ArrayBuffer[Int], chosen:Int): Double = {
    val cnt = new Array[Int](4)
    for (j <- 0 until X.length) {
      val a = if (Z(j) == 1) true else false
      val b = judge(X, Y, chosen, j)
      if (!a && !b) {
        cnt(0) += 1
      }
      else if (!a && b) {
        cnt(1) += 1
      }
      else if (a && !b) {
        cnt(2) += 1
      }
      else if (a && b) {
        cnt(3) += 1
      }
    }

    val p = new Array[Double](4)
    for (i <- 0 until 4) {
      if (cnt(i) == 0) p(i) = 0.0
      else p(i) = cnt(i) * 1.0 / X.length
    }

    var ans:Double = 0.0
    for (i <- 0 until 4) {
      breakable {
        if ((p(i) - 0.0).abs < 1e-6 || p(i) < 0) break()
        ans += (p(i) * Math.log(p(i)))
      }
    }
    return -ans
  }

  // U(X | Y)
  def U(X:ArrayBuffer[Int], Y:ArrayBuffer[Int]): Double = {
    if (X==null || Y==null || X.length <= 0 || Y.length <= 0) return 0.0
    val HEX: Double = H(X)
    if (HEX.abs < 1e-6) return 1.0
    return (HEX + H(Y) - H(X, Y)) / HEX
  }

  //Z在（X, Y)情况下 U(Z | (X, Y))
  def U(Z:ArrayBuffer[Int], X:ArrayBuffer[Int], Y:ArrayBuffer[Int], chosen:Int): Double = {
    val HEZ: Double = H(Z)
    if (HEZ.abs < 1e-6) return 1.0
    return (HEZ + H(X, Y, chosen) - H(Z, X, Y, chosen)) / HEZ
  }


  def main(args: Array[String]) {
    val infile = Source.fromFile("data/oral2_test.txt")
    val outfile = new PrintWriter(new File("data/test_result.txt"))

    val conf = new SparkConf().setAppName("SimpleGraphX").setMaster("local[*]")
    val sc = new SparkContext(conf)


    // 打开文件并存储入数组
    var cnt = 3
    for (line <- infile.getLines) {
      length = line.length()
      breakable {
        if (n == 0) {
          n = Integer.parseInt(line)
          break()
        }
        var now_array = new ArrayBuffer[Int]()
        for(str <- line) {
          now_array += str - '0'
        }
        //outfile.println(line)
        matrixLAPP += Map(cnt -> now_array)
        cnt += 1
      }
    }

    //二阶
    /*val rdd1 = sc.parallelize(matrixLAPP)
    val rdd2 = sc.parallelize(matrixLAPP)
    val rdd3 = rdd1.cartesian(rdd2)
    val rdd4 = rdd3.map{x =>
        val a = x._1
        val b = x._2
        breakable{
            if (a.keys.head >= b.keys.head) break()
            val mie = U(a.values.head, b.values.head)
            val result = List(a.keys.head, b.keys.head, mie.formatted("%.6f"))
            println(result)
        }
    }.count()
    //rdd4.foreach(x => println(x))
    //rdd4.coalesce(1,true).saveAsTextFile("F:/LAPP/test3.txt")
*/

    //sanjie
    val rdd1 = sc.parallelize(matrixLAPP)
    val rdd2 = sc.parallelize(matrixLAPP)
    val rdd3 = rdd1.cartesian(rdd2).cartesian(rdd2)
    var rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 1)
          if (resLapp < 0.6) break()
          val result = List("#1#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 2)
          if (resLapp < 0.6) break()
          val result = List("#2#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 3)
          if (resLapp < 0.6) break()
          val result = List("#3#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 4)
          if (resLapp < 0.6) break()
          val result = List("#4#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 51)
          if (resLapp < 0.6) break()
          val result = List("#51#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 52)
          if (resLapp < 0.6) break()
          val result = List("#52#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 61)
          if (resLapp < 0.6) break()
          val result = List("#61#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 62)
          if (resLapp < 0.6) break()
          val result = List("#62#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 7)
          if (resLapp < 0.6) break()
          val result = List("#7#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()

    rdd4 = rdd3.map{x =>
      val i = x._1._1
      val j = x._1._2
      val k = x._2
      breakable{
        if (i.keys.head >= j.keys.head) break()
        if (i.keys.head == k.keys.head || j.keys.head == k.keys.head) break()
        val res1: Double = U(k.values.head, i.values.head)
        val res2: Double = U(k.values.head, j.values.head)
        if (res1 < 0.3 && res2 < 0.3) {
          val resLapp: Double = U(k.values.head, i.values.head, j.values.head, 8)
          if (resLapp < 0.6) break()
          val result = List("#8#", k.keys.head, i.keys.head, j.keys.head, res1.formatted("%.6f"), res2.formatted("%.6f"), resLapp.formatted("%.6f"))
          //println(k.keys.head + " " + i.keys.head + " " + j.keys.head + " " + res1.formatted("%.6f") + " " + res2.formatted("%.6f") + " " + resLapp.formatted("%.6f"))
          println(result)
        }
      }
    }.count()


    infile.close
    outfile.close()
  }
}
