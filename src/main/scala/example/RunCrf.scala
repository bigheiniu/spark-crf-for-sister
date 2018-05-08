import com.intel.imllib.crf.nlp._
import com.intel.imllib.util._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object RunCrf extends App {

  override def main(args: Array[String]): Unit = {
    if (args.length != 5) println(" FuckRun <model> <fileDir> <tableDir> <outputdir> <partitions>")
    val modelFile = args(0)
    val excutFile = args(1)
    val fileFlag = 0
    val tableDir = args(2)
    val outputDir = args(3)
    val partion = args(4).toInt


    val t1 = System.nanoTime()
    //将 excuteFile 下面的所有内容转化成 array RDD
    val spark = SparkSession.builder().appName("crfRun").getOrCreate()
    // model stored at hdfs
    val model = CRFModel.loadArray(spark.sparkContext.textFile(modelFile).collect())
    val (fileRdd, fileNames) = Preprocess.preprocess(spark,excutFile,partion)
    // 获取侯建特征的词表
    val table = FeatureExtract.get_table_content()
    val bcTable = spark.sparkContext.broadcast(table)
    val featureRdd = fileRdd.map(arr => FeatureExtract.getSequece(arr._2,bcTable.value,fileFlag,arr._1.toInt))

    val results = model.setNBest(10).setVerboseMode(VerboseLevel1).predict(featureRdd)
    // token的位置位于tags第一位
    val resultsTofile = results.map(arr => arr.toArray.map(thu => (thu.tags(0), thu.label))).zip(fileRdd.map(_._1)).map(_.swap)
    // these data stored at hdfs
    println("save file in local filesystem may cause a lot of time, please be patient")
    FileOutput.write2file(outputDir,fileNames,resultsTofile)
    println(s"thanks for usage, the results are stored at $outputDir")
    val t2 = System.nanoTime()
    val time = (t2 - t1) / 1e9d
    println(s"the time is $time")
    bcTable.unpersist(blocking = false)
    spark.stop()
  }

}
