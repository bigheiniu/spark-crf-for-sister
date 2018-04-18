package com.intel.imllib.util

import org.apache.spark.rdd.RDD
import java.io.{PrintWriter,File}
import scala.io.Source
object FileOutput {
  def write2file(outputDir: String, fileNames: Array[String], rDD: RDD[(Long, Array[(String,String)])]): Unit = {
    val onefileRDD = rDD.groupByKey()
    val onefile = onefileRDD.collect()
    val output = onefile.map( t => (t._1, t._2.toArray.map(arr => arr.map(thu => thu._1 + "|" + thu._2).mkString("\t")).mkString("\n")))
    output.foreach(arr => {
      val fileIndex = arr._1
      val content = arr._2
      val fileName = fileNames(fileIndex.toInt)
      val dirName = outputDir + fileName
      val writer = new PrintWriter(new File(dirName))
      writer.write(arr._2)
      writer.close()
    })
  }
}
