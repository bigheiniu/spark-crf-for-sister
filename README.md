## 基于 crf 的细菌命名实体识别
--------
项目主体代码 [__crf__](https://github.com/Intel-bigdata/CRF-Spark) 算法实现来自intel bigdata 项目, 在原来基础上接入了文章分词功能, [__英文分词__](https://github.com/databricks/spark-corenlp) 使用的是 databricks 的分词服务.

spark 计算依赖于 RDD, 下一步改进可以迁移到 dataframe 上.

目前经过测试, 可以在 [__云上__](https://console.qingcloud.com) 运行.
