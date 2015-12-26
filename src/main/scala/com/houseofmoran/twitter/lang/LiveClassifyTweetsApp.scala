package com.houseofmoran.twitter.lang

import java.io._

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.{StreamingContext, Minutes}
import org.apache.spark.{SparkConf, SparkContext}

object LiveClassifyTweetsApp {

  def readModel(file: File) = {
    val objIn = new ObjectInputStream(new FileInputStream(file))
    try {
      objIn.readObject().asInstanceOf[PipelineModel]
    }
    finally {
      objIn.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LiveClassifyTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val batchInterval = Minutes(1)
    val ssc = new StreamingContext(sc, batchInterval)
    val sqlContext = SQLContext.getOrCreate(sc)
    import sqlContext.implicits._

    val modelFileIn = new File(args(0))
    val model = readModel(modelFileIn)

    val treeModel = model.stages(3).asInstanceOf[DecisionTreeClassificationModel]

    val twitterStream = TwitterStream.fromAuth(ssc, args(1), args(2), args(3), args(4))

    val tweetStream = TwitterStream.mapToTweetStream(twitterStream)

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision")
    val windowSize = batchInterval * 5
    val slideDuration = batchInterval
    tweetStream.window(windowSize, slideDuration).foreachRDD( (tweetsRDD, time) => {
      val tweetsDF = tweetsRDD.toDF()
      tweetsDF.show()

      val normalisedDF = HasMedia.normalise(tweetsDF)

      val predictions = model.transform(normalisedDF).cache()

      val accuracy = evaluator.evaluate(predictions)
      println(s"Accuracy = ${accuracy}, Test Error = ${1 - accuracy}")
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
