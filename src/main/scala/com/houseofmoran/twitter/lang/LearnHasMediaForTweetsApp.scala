package com.houseofmoran.twitter.lang

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{StringIndexer, Tokenizer, Word2Vec}
//import org.apache.spark.ml.Pipeline
//import org.apache.sparkml.classification.DecisionTreeClassifier
//import org.apache.sparkml.feature.StringIndexer
//import org.apache.sparkml.evaluation.MulticlassClassificationEvaluator
//import org.apache.sparkmllib.util.MLUtils
//import org.apache.sparksql.Row
//import org.apache.spark.feature.Tokenizer

object LearnHasMediaForTweetsApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LearnHasMediaForTweetsApp").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = SQLContext.getOrCreate(sc)

    val tweetsDf = sqlContext.read.parquet("tweets.consolidated.parquet").cache()

    tweetsDf.printSchema()
    tweetsDf.show()

    tweetsDf.groupBy("hasMedia").count().show()

    val boolToDouble : (Boolean => Double) = (b : Boolean) => if (b) 1.0 else 0.0
    val mlDf = tweetsDf.withColumn("label", callUDF(boolToDouble, DoubleType, col("hasMedia")))

    mlDf.printSchema()
    mlDf.show()

    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    val word2Vec = new Word2Vec().setInputCol("words").setOutputCol("wordvecs")
      .setVectorSize(3)
      .setMinCount(0)
    val word2VecModel = word2Vec.fit(tokenizer.transform(mlDf))
    val mlWord2VecDf = word2VecModel.transform(tokenizer.transform(mlDf))

    mlWord2VecDf.printSchema()
    mlWord2VecDf.show()

    val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel")
      .fit(mlWord2VecDf)

    val Array(training : DataFrame, test : DataFrame) = mlWord2VecDf.randomSplit(Array(0.7, 0.3), seed = 1)

    val treeClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("wordvecs")
    val pipeline = new Pipeline().setStages(Array(labelIndexer, treeClassifier))

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

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeClassificationModel]

    println(model)
    println(treeModel.toDebugString)
  }
}
