package com.houseofmoran.twitter.lang.hasmedia

import java.io.{File, FileOutputStream, ObjectOutputStream}

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, Tokenizer, Word2Vec}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object LearnFromTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LearnHasMediaForTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val sampleFraction = args(0).toDouble
    val modelFileOut = new File(args(1))

    val tweetsDf = sqlContext.read.parquet("tweets.consolidated.parquet")
      .sample(false, sampleFraction, 1)
      .cache()

    tweetsDf.printSchema()
    tweetsDf.show()

    tweetsDf.groupBy("hasMedia").count().show()

    val mlDf = HasMedia.normalise(tweetsDf)
    val Array(training : DataFrame, test : DataFrame) = mlDf.randomSplit(Array(0.7, 0.3), seed = 1)

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("wordvecs")
      .setVectorSize(3)
      .setMinCount(0)
    val word2VecModel = word2Vec.fit(tokenizer.transform(training))

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")

    val treeClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("wordvecs")
    val pipeline = new Pipeline().setStages(Array(tokenizer, word2VecModel, labelIndexer, treeClassifier))

    val model = pipeline.fit(training)

    val predictions = model.transform(test).cache()

    predictions.printSchema()
    predictions.show()
    predictions.registerTempTable("predictions")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy = ${accuracy}, Test Error = ${1 - accuracy}")

    val correct = sqlContext.sql("SELECT * FROM predictions WHERE indexedLabel = prediction")
    println (correct.count())
    correct.show()

    val incorrect = sqlContext.sql("SELECT * FROM predictions WHERE indexedLabel != prediction")
    println (incorrect.count())
    incorrect.show()

    val treeModel = model.stages(3).asInstanceOf[DecisionTreeClassificationModel]

    println(model)
    println(treeModel.toDebugString)

    val objOut = new ObjectOutputStream(new FileOutputStream(modelFileOut))
    try {
      objOut.writeObject(model)
    }
    finally {
      objOut.flush()
      objOut.close()
    }
  }
}
