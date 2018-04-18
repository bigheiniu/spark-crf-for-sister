package com.intel.imllib.crf.nlp

import java.io.{DataInputStream, DataOutputStream, FileInputStream, FileOutputStream}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.intel.imllib.crf.nlp._

object CrfModelTrain {
    def modelTrain(templateFile: String, trainFile: String, testFile: String,sc: SparkContext,flag: Boolean):Unit = {

      val templates: Array[String] = scala.io.Source.fromFile(templateFile).getLines().filter(_.nonEmpty).toArray
      val trainRDD: RDD[Sequence] = sc.textFile(trainFile).filter(_.nonEmpty).map(Sequence.deSerializer)

      val model: CRFModel = CRF.train(templates, trainRDD, 0.25, 1, 100, 1E-3, "L1")

      val testRDD: RDD[Sequence] = sc.textFile(testFile).filter(_.nonEmpty).map(Sequence.deSerializer)


      /**
        * an example of model saving and loading
        */
//      new java.io.File("target/model").mkdir()
//      //model save as String
//      new java.io.PrintWriter("target/model/model1") { write(CRFModel.save(model)); close() }
//      val modelFromFile1 = CRFModel.load(scala.io.Source.fromFile("target/model/model1").getLines().toArray.head)
//      // model save as RDD
//      sc.parallelize(CRFModel.saveArray(model)).saveAsTextFile("target/model/model2")
//      val modelFromFile2 = CRFModel.loadArray(sc.textFile("target/model/model2").collect())
//      // model save as BinaryFile

      /**
        * still use the model in memory to predict
        */
      val results: RDD[Sequence] = model.setNBest(10)
        .setVerboseMode(VerboseLevel1)
        .predict(testRDD)

      val TP = results
        .zipWithIndex()
        .map(_.swap)
        .join(testRDD.zipWithIndex().map(_.swap))
        .map(_._2)
        .map(x => x._1.Baccompare(x._2))
        .reduce(_ + _)
      val TPFN = testRDD.map(_.toArray.count(s => s.label == "B-bacteria")).reduce(_ + _)
      val TPFP = results.map(_.toArray.count(s => s.label == "B-bacteria")).reduce(_ + _)
      val P = TP / (TPFP * 1.0)
      val R = TP / (TPFN * 1.0)
      val F = 2 * P * R /( P + R)

      println(s"P is : $P")
      println(s"R is : $R")
      println(s"F is : $F")
      println(s"the train is stop")

      //判断是不是仅仅训练模板, 如果是的话就结束训练, 否则就返回训练好的模板
        val path = "target/model/model3"
        new java.io.File(path).mkdir()
        CRFModel.saveBinaryFile(model, path)
        println(s"the model have been saved at $path")
        sc.stop()
  }

}
