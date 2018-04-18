#!/usr/bin/env bash
~/spark-2.1.1-bin-hadoop2.7/bin/spark-submit \
    --class CRFExample \
    --master local[*] \
    target/scala-2.11/imllib_2.11-0.0.1.jar \
    data/crf/Mytemplate \
    results/train.data \
    results/test.data
