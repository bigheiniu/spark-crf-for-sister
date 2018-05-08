package com.intel.imllib.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.functions.explode


import java.io.File

//输入一行的文字, 需要进行分词处理,
// 返回作文编号和内容的索引 => 对应关系是 index -> array(words)
// 测试通过, 作文进行测试, 分词之后组合起来还是原来的文章
object Preprocess {
//  case class fileRdd(fileName: String, fileContent: RDD[(Long, Array[String])])
  //返回
   def preprocess(spark: SparkSession,dir: String,partion: Int): (RDD[(Long, Array[String])],Array[String]) = {
    import spark.implicits._
     val fileRdd = spark.sparkContext.wholeTextFiles(dir, partion).cache()
     // linux 文件系统
     val fileNames = fileRdd.map(arr => arr._1.split("/").last).collect()
     val input = fileRdd.zipWithIndex().map(arr => (arr._2,arr._1._2)).toDF("fileIndex","text")

    val output = input
      .select('fileIndex, explode(ssplit('text)).as('sen))
      .select('fileIndex, tokenize('sen).as('words))

     output.show()
     val rddOutput = output.map(arr => (arr.getAs[Long]("fileIndex"),Array(arr.getAs[Seq[String]]("words"):_*))).rdd
//    val rddOutput = output.map(arr => arr.getAs[Seq[Seq[String]]]("words")).rdd
    (rddOutput,fileNames)
  }

}


