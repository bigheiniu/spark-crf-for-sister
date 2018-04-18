import com.intel.imllib.crf.nlp._
import com.intel.imllib.util._
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object FuckRun {

  def main(args: Array[String]): Unit = {
    if (args.length != 5) println(" FuckRun <model> <fileDir> <tableDir> <outputdir> <partitions>")
    val modelFile = args(0)
    val excutFile = args(1)
//    // fileFlag = 0 表示原始数据, 需要对数据进行分句然后分词
    val fileFlag = 0
    val tableDir = args(2)
    val outputDir = args(3)
      val partion = args(4).toInt
//    val modelFile = "target/model/model3"
//    val excutFile = "/Users/bigheiniu/course/graduate_pro/spark-crf/spar-crf-data/test/"
    // fileFlag = 0 表示原始数据, 需要对数据进行分句然后分词
//    val fileFlag = 0
//    val tableDir = "/Users/bigheiniu/course/graduate_pro/spark-crf/imllib-spark/table/"
//    val outputDir = "/Users/bigheiniu/course/graduate_pro/spark-crf/imllib-spark/target/results/"
    val t1 = System.nanoTime()
    val model = CRFModel.loadBinaryFile(modelFile)
    //将 excuteFile 下面的所有内容转化成 array RDD
    val spark = SparkSession.builder().appName("crf").getOrCreate()
    val (fileRdd, fileNames) = Preprocess.preprocess(spark,excutFile,partion)
    // 获取侯建特征的词表
    val table = FeatureExtract.get_table_content(tableDir)
    val bcTable = spark.sparkContext.broadcast(table)
    val featureRdd = fileRdd.map(arr => FeatureExtract.getSequece(arr._2,bcTable.value,fileFlag,arr._1.toInt))

    val results = model.setNBest(10).setVerboseMode(VerboseLevel1).predict(featureRdd)
    // token的位置位于tags第一位
    val resultsTofile = results.map(arr => arr.toArray.map(thu => (thu.tags(0), thu.label))).zip(fileRdd.map(_._1)).map(_.swap)
    FileOutput.write2file(outputDir,fileNames,resultsTofile)
    println(s"thanks for usage, the results are stored at $outputDir")
    val t2 = System.nanoTime()
    val time = (t2 - t1) / 1e9d
    println(s"the time is $time")
      bcTable.unpersist(blocking = false)

    val arrayBuffer = new ArrayBuffer[String]()
    (0 to 10).foreach(
      num => arrayBuffer += "fuck: " + num + "turns"
    )
    spark.sparkContext.parallelize(arrayBuffer.toArray.mkString("\n")).saveAsTextFile("hdfs://192.168.0.7:9000/fuck/text.txt")



////    preprocess.map(arr => arr)

//    val fileNames = preprocess.map(_._1)
//    val result = preprocess.map(arr => arr._2.map(thu => FeatureExtract.getSequece(thu,bcTable.value,fileFlag,0)))
//    val predict = result.map(arr => model.setNBest(10).setVerboseMode(VerboseLevel1).predict(arr))
//    val labelsTokens = predict.map(_.map(arr => arr.toArray.map(th => (th.label,th.tags(0)))))
//    val fuck = labelsTokens.map(arr => arr.collect())
  }

}
