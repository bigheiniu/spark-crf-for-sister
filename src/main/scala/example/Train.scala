import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import com.databricks.spark.corenlp.functions._
import com.intel.imllib.crf.nlp._
import com.intel.imllib.util._
import org.apache.spark.sql.SparkSession
import

object Train extends App {

  override def main(args: Array[String]) {


//      val t1 = System.nanoTime()
//    val templateFile = "data/crf/Mytemplate"
//    val trainFile = "data/crf/train_result.txt"
//    val testFile = "data/crf/test_result.txt"
    val templateFile = args(0)
      val trainFile = args(1)
      val testFile = args(2)
//      val tableFile = args(3)
      val parition = args(3).toInt
    val t1 = System.nanoTime()
    val conf = new SparkConf().setAppName("CRFExample")
    val sc = new SparkContext(conf)
    val templates: Array[String] = sc.textFile(templateFile).collect().filter(_.nonEmpty)
      val bcTem = sc.broadcast(templates)
      val trainRDD = sc.textFile(trainFile,parition).filter(_.nonEmpty).map(_.split("\t"))
    val table = FeatureExtract.get_table_content("table/")
    val bcTable = sc.broadcast(table)
    val trainResult = trainRDD.map(arr => FeatureExtract.getSequece(arr,bcTable.value,1,0)).cache()
    val testRDD =  sc.textFile(testFile,parition).filter(_.nonEmpty).map(_.split("\t"))
    val testResult= testRDD.map(arr => FeatureExtract.getSequece(arr,bcTable.value,1,0)).cache()

    val model: CRFModel = CRF.train(bcTem.value, trainResult, 0.25, 1, 100, 1E-3, "L1")


    val path = "target/model/model3"
    new java.io.File(path).mkdir()
    CRFModel.saveBinaryFile(model, path)
    val modelFromFile3 = CRFModel.loadBinaryFile(path)

    val results: RDD[Sequence] = modelFromFile3.setNBest(10)
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
        println(s"P is : $P and ${TP} / ${TPFP}")
        println(s"R is : $R and ${TP} / ${TPFN}")
        println(s"F is : $F")
        println(s"the model save at $path")
    val t2 = System.nanoTime()
    val time = (t2 - t1) / 1e9d
    println(s"the time is $time")
//        println()

        sc.stop()

  }
}