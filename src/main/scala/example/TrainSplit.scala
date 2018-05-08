import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import com.intel.imllib.crf.nlp._
import com.intel.imllib.util._
import java.io._

import scala.collection.mutable.ArrayBuffer

object TrainSplit {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) println(" FuckRun <template> <trainfile> <testfile> <table>")
    val templateFile = args(0)
    val trainFile = args(1)
    val testFile = args(2)
    val tableFile = args(3)
    val partition = args(4).toInt
    val conf = new SparkConf().setAppName("CRFTrainSplit").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val templates: Array[String] = scala.io.Source.fromFile(templateFile).getLines().filter(_.nonEmpty).toArray
    val trainRDD = sc.textFile(trainFile,partition).filter(_.nonEmpty).map(_.split("\t"))
    val table = FeatureExtract.get_table_content()
    val bcTable = sc.broadcast(table)
    val trainResult = trainRDD.mapPartitions(thu => thu.map(arr => FeatureExtract.getSequece(arr,bcTable.value,1,0))).zipWithIndex().cache()
    val testRDD =  sc.textFile(testFile,partition).filter(_.nonEmpty).map(_.split("\t"))
    val testResult= testRDD.mapPartitions(arr => arr.map(thu => FeatureExtract.getSequece(thu,bcTable.value,1,0))).cache()

    val arrayBuffer = new ArrayBuffer[String]()
    arrayBuffer += "P|R|F|time|num"

    val lenth = trainResult.count().asInstanceOf[Int]
    (1000 until  lenth by 1000).foreach(
      num => {
        val trainSplit = trainResult.filter(arr => arr._2 <= num).map(_._1)
        val len = trainSplit.count()
        println(s"the length of trainsplit is $len")
        val t1 = System.nanoTime()
        val model: CRFModel = CRF.train(templates, trainSplit, 0.25, 1, 100, 1E-3, "L1")
        val results: RDD[Sequence] = model.setNBest(10)
          .setVerboseMode(VerboseLevel1)
          .predict(testResult)
        val TP = results
          .zipWithIndex()
          .map(_.swap)
          .join(testResult.zipWithIndex().map(_.swap))
          .map(_._2)
          .map(x => x._1.Baccompare(x._2))
          .reduce(_ + _)
        val TPFN = testResult.map(_.toArray.count(s => s.label == "B-bacteria")).reduce(_ + _)
        val TPFP = results.map(_.toArray.count(s => s.label == "B-bacteria")).reduce(_ + _)
        //    val total = testRDD.map(_.toArray.length).reduce(_ + _)
        val P = TP / (TPFP * 1.0)
        val R = TP / (TPFN * 1.0)
        val F = 2 * P * R /( P + R)
        val t2 = System.nanoTime()
        val time = (t2 - t1)/1e9d
        val mkstrings = s"$P|$R|$F|$time|$len's sample"
        println(s"the time for $num is $time")
        println(mkstrings)
        arrayBuffer += mkstrings
      }
    )
    testResult.unpersist()
    trainResult.unpersist()
    sc.stop()
    if (arrayBuffer.length > 2) {
      val outputString = arrayBuffer.mkString("\n")
      val t1 = System.nanoTime()
      val pw = new PrintWriter(new File(s"target/TrainSplitResults_${t1}.txt"))
      pw.write(outputString)
      pw.close()
    }
    println("the train split is stop")


  }
}
