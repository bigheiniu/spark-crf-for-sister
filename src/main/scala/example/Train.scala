import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.intel.imllib.crf.nlp._
import com.intel.imllib.util._


object Train extends App {

  override def main(args: Array[String]) {


    val templateFile = args(0)
    val trainFile = args(1)
    val testFile = args(2)
    val parition = args(3).toInt
    val savefileResult = args(4)
    val saveModel = args(4)
    val t1 = System.nanoTime()
    val conf = new SparkConf().setAppName("CRF train")
    val sc = new SparkContext(conf)
    val templates: Array[String] = sc.textFile(templateFile).collect().filter(_.nonEmpty)
    val bcTem = sc.broadcast(templates)
    val trainRDD = sc.textFile(trainFile,parition).filter(_.nonEmpty).map(_.split("\t"))
    val table = FeatureExtract.get_table_content()
    val bcTable = sc.broadcast(table)
    val trainResult = trainRDD.map(arr => FeatureExtract.getSequece(arr,bcTable.value,1,0)).cache()

    val testRDD =  sc.textFile(testFile,parition).filter(_.nonEmpty).map(_.split("\t"))

    val testResult= testRDD.map(arr => FeatureExtract.getSequece(arr,bcTable.value,1,0)).cache()

    val model: CRFModel = CRF.train(bcTem.value, trainResult, 0.25, 1, 100, 1E-3, "L1")
    try {
      CRFModel.loadArray(sc.textFile(saveModel).collect())
    } catch {
      case _: Throwable => {
        println( "problem with model file save path, please make sure model file path is correct")
        System.exit(-1)
      }
    }

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

      //        println(s"the model save at $path")
      val t2 = System.nanoTime()
      val time = (t2 - t1) / 1e9d
      val arr = Array(s"P is : $P and ${TP} / ${TPFP} \n",s"R is : $R and ${TP} / ${TPFN} \n",s"F is : $F \n",s"the time is $time")

      sc.parallelize(arr).saveAsTextFile(savefileResult)
      //        println()

      sc.stop()
    }

  }
