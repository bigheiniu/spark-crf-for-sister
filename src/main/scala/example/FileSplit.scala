import java.io.{File, PrintWriter}

import com.intel.imllib.crf.nlp.{CRFModel, VerboseLevel1}
import com.intel.imllib.util.{FeatureExtract, FileOutput, Preprocess}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object FileSplit {
  def main(args: Array[String]): Unit = {
    if (args.length != 6) println(" FuckRun <model> <fileDir> <tableDir> <outputdir> <partition> <split>")
    val modelFile = args(0)
    val excutFile = args(1)
    //    // fileFlag = 0 表示原始数据, 需要对数据进行分句然后分词
    val fileFlag = 0
    val tableDir = args(2)
    val outputDir = args(3)
    val partion = args(4).toInt
    val model = CRFModel.loadBinaryFile(modelFile)
    //将 excuteFile 下面的所有内容转化成 array RDD
    val spark = SparkSession.builder().appName("crfFileSplit").getOrCreate()
    val (fileRdd, fileNames) = Preprocess.preprocess(spark,excutFile,partion)
    val fileNum = fileNames.length
    val testCase = Array(1000,2000,3000)
    // 获取侯建特征的词表

    testCase.foreach(number => {
      val t1 = System.nanoTime()
      val table = FeatureExtract.get_table_content()
      val bcTable = spark.sparkContext.broadcast(table)
      val middleFileRdd = fileRdd.filter(arr => arr._1 < number.toLong)
      val featureRdd = middleFileRdd.map(arr => FeatureExtract.getSequece(arr._2,bcTable.value,fileFlag,arr._1.toInt)).cache()
      val t2 = System.nanoTime()
      val timePre = (t2 - t1) /1e9d
      val arrayBuffer = new ArrayBuffer[String]()
      arrayBuffer += "fileNum|time|pretime|predictime|"

      val t3 = System.nanoTime()
      val results = model.setNBest(10).setVerboseMode(VerboseLevel1).predict(featureRdd)
      val dfResult = results.flatMap(arr => arr.toArray.map(thu => (thu.tags(0), thu.label))).count()
      val t4 = System.nanoTime()
      val predictTime = (t4 - t3) / 1e9d
      val alltime = predictTime + timePre
      val output = s"$fileNum|$alltime|$timePre|$predictTime"
      arrayBuffer += output

      if (arrayBuffer.length > 1) {
        val outputString = arrayBuffer.mkString("\n")
        // 要除以3
        val pw = new PrintWriter(new File(s"target/FilesplitResults_${partion / 3}_$t2.txt"))
        pw.write(outputString)
        pw.close()
      }
      featureRdd.unpersist()
      bcTable.unpersist()
      println(s"thanks for usage, the results are stored at $outputDir")


    })
//    val table = FeatureExtract.get_table_content(tableDir)
//    val bcTable = spark.sparkContext.broadcast(table)
//    val featureRdd = fileRdd.map(arr => FeatureExtract.getSequece(arr._2,bcTable.value,fileFlag,arr._1.toInt))
//    val t2 = System.nanoTime()
//    val timePre = (t2 - t1) /1e9d
//    val arrayBuffer = new ArrayBuffer[String]()
//    arrayBuffer += "fileNum|time|pretime|predictime|"

    //(split until fileNum by split).foreach(
    //  num => {
    //    val t3 = System.nanoTime()
    //    val runRdd = featureRdd./*filter(arr => arr._2 <= num).*/mapPartitions(arr => arr.map(_._1))
    //    val results = model.setNBest(10).setVerboseMode(VerboseLevel1).predict(runRdd)
    //    val len = results.map(arr => arr.toArray.map(thu => (thu.tags(0), thu.label))).count()
    //    val t4 = System.nanoTime()
    //    val predictTime = (t4 - t3) / 1e9d
    //    val alltime = predictTime + timePre
    //    val output = s"$num|$alltime|$timePre|$predictTime"
    //    println(output)
    //    arrayBuffer += output
    //  }
    //)
    // token的位置位于tags第一位

//        val t3 = System.nanoTime()
//        val results = model.setNBest(10).setVerboseMode(VerboseLevel1).predict(featureRdd)
//      val dfResult = results.flatMap(arr => arr.toArray.map(thu => (thu.tags(0), thu.label))).count()
//        val t4 = System.nanoTime()
//        val predictTime = (t4 - t3) / 1e9d
//        val alltime = predictTime + timePre
//        val output = s"$fileNum|$alltime|$timePre|$predictTime"
//        println(output)
//        arrayBuffer += output
//
//    if (arrayBuffer.length > 1) {
//      val outputString = arrayBuffer.mkString("\n")
//        // 要除以3
//      val pw = new PrintWriter(new File(s"target/FilesplitResults_${partion / 3}_$t2.txt"))
//      pw.write(outputString)
//      pw.close()
//    }
//    featureRdd.unpersist()
//    bcTable.unpersist()
    spark.stop()
//    println(s"thanks for usage, the results are stored at $outputDir")
  }
}
