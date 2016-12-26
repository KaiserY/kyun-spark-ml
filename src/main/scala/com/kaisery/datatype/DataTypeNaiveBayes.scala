package com.kaisery.datatype

import com.kaisery.common.{DataType, Util}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataTypeNaiveBayes {

    val spark: SparkSession = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.driver.extraJavaOptions", "-Xss100m")
        .config("spark.executor.memory", "6g")
        .master("local[2]")
        .getOrCreate()

    import spark.implicits._

//    val trainingDataset = DataTypeUtil.makeDataset(spark)
    val trainingRDD = DataTypeUtil.makeRDD(spark)

    val dataFrame = trainingRDD.map(d => {
        (d.label, d.content, Util.string2Vector(d.content, 100))
    }).toDF("label", "content", "features")

    val Array(trainingDataFrame, testDataFrame) = dataFrame.randomSplit(Array(0.75, 0.25))

    val model = train(trainingDataFrame)

    def main(args: Array[String]) {
        test(testDataFrame)
    }

    def test(testDataFrame: DataFrame): Unit = {

        val predictions = model.transform(testDataFrame)

        val multiclassClassificationEvaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")

        val accuracy = multiclassClassificationEvaluator.evaluate(predictions)

        val predictionAndLabel = predictions.select("prediction", "label").map(r => (r.getDouble(0), r.getDouble(1))).toJavaRDD
        val metrics = new MulticlassMetrics(predictionAndLabel)

        val stringBuilder = new scala.collection.mutable.StringBuilder()

        stringBuilder.append("\n")
        stringBuilder.append(accuracy)
        stringBuilder.append("\n\n")
        stringBuilder.append(metrics.confusionMatrix.toString(10000, 10000))
        stringBuilder.append("\n\n")
        DataType.values.foreach(t => stringBuilder.append(t.toString.padTo(20, " ").mkString + "-->\t" + metrics.precision(t.id.toDouble) + "\n"))
        stringBuilder.append("\n")

        println(stringBuilder)
    }

    def train(trainingDataFrame: DataFrame): PipelineModel = {
        val naiveBayes = new NaiveBayes()
            .setSmoothing(1.0)
            .setModelType("multinomial")
            .setLabelCol("label")
            .setFeaturesCol("features")
        val pipeline = new Pipeline()
            .setStages(Array(naiveBayes))

        pipeline.fit(trainingDataFrame)
    }

    def learn(args: String*): Array[String] = {

        val test = spark.createDataFrame(args.map(content => {
            (content, Util.string2Vector(content, 100))
        })).toDF("content", "features")

        model.transform(test)
            .select("content", "probability", "prediction")
            .collect()
            .map {
                case Row(content: String, prob: Vector, prediction: Double) =>
                    val predictionDataType = DataType.apply(prediction.toInt)
                    val predictionString = s"$content --> $predictionDataType\n\n"
                    val probArray = prob.toArray
                    val probabilityString = Array.tabulate(probArray.length) { i => DataType(i).toString.padTo(20, ' ') + "-->\t" + probArray(i) + "\n" }
                    predictionString ++ probabilityString.mkString
                case _ => "error"
            }
    }
}
