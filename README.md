## 基于 crf 的细菌命名实体识别
--------
项目主体代码 [__crf__](https://github.com/Intel-bigdata/CRF-Spark) 算法实现来自intel bigdata 项目, 在原来基础上接入了文章分词功能, [__英文分词__](https://github.com/databricks/spark-corenlp) 使用的是 databricks 的分词服务.

spark 计算依赖于 RDD, 下一步改进可以迁移到 dataframe 上.

目前经过测试, 可以在 [__云上__](https://console.qingcloud.com) 运行.



Linux/Macos: 

1. prerequiest:

   - install  sbt=0.13.16, spark>2.1, scala=2.11

   - install [spark corenlp](https://github.com/databricks/spark-corenlp) package for word segment, follow [install instruction](https://github.com/Intel-bigdata/imllib-spark) _Use as a dependency_ way   (sorry currently, I met some problem in using sbt assembly)

     

2. install this project

   - 

     ```shell
     git clone https://github.com/bigheiniu/spark-crf-for-sister
     cd spark-crf-for-sister
     sbt package
     ```

     This will compile the project for usage, you can transport it in cloud.

     

3. run progrm

   - Firstly you should part of files in hdfs file system. Files whose name contain HDFS mean they stored in hdfs. These files should use these commands to store file in hdfs file system in Qingcloud(cloud system).

   ```shell
   #get sudo first
   sudo su 
   hdfs dfs -put localFilesystemFile.txt /path/to/hdfs/file/system
   ```

   

   - If you want to tran the model, use command like this

     ```shell
     #!/usr/bin/env bash
     spark-submit \
         --class Train  \
         --master local[*] \
         templateFile \ 
         yourcompile.jar \
         trainHDFSfile \
         testHDFSfile \ 
         6 \ #spark partition number
         testResultHDFSfile \
         saveMoedelHDFSfile
     ```

     This command save crf model trained by tranHDFSfile in hdfs file systems. And the performance of this program will be saved at testResultHDFSfile. Three evaluation metrics are P/R/F score.

     - If you want to use already trained model to do pattern recognition.

       ```shell
       #!/usr/bin/env bash
       spark-submit \
       	--class RunCrf \	
           --master local[*] \
           yourcompiled.jar \
       	modelHDFSfile \
       	preprocessFileDir \
       	outputDir \
       	#parition number
          8 
       ```

        model saved at hdfs file systems, the input file you can just put at client, which means just storing at local file system.





-------



