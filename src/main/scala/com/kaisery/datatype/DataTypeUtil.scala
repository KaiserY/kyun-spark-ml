package com.kaisery.datatype

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataTypeUtil {

    def makeDataset(spark: SparkSession): Dataset[DataWithLabel] = {
        import spark.implicits._

        var dataset = spark.emptyDataset[DataWithLabel]

        DataTypeSeq.dataTypeSeqMap().values foreach {
            seq => dataset = dataset.union(spark.createDataset(seq))
        }

        dataset
    }

    def makeRDD(spark: SparkSession): RDD[DataWithLabel] = {
        import spark.implicits._

        var rdd = spark.sparkContext.emptyRDD[DataWithLabel]

        DataTypeSeq.dataTypeSeqMap().values foreach {
            seq => rdd = rdd.union(spark.sparkContext.parallelize(seq))
        }

        rdd
    }
}
