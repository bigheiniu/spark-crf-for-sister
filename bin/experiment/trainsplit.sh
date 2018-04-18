#!/usr/bin/env bash
~/spark-2.1.1-bin-hadoop2.7/bin/spark-submit \
--packages databricks:spark-corenlp_2.11:0.3.0-SNAPSHOT	  --class TrainSplit  \
    --master local[*] \
    --driver-memory 128g \
    target/scala-2.11/imllib_2.11-0.0.1.jar  \
    data/crf/Mytemplate \
    data/crf/train_result.txt \
    data/crf/test_result.txt

