package com.houseofmoran.twitter.lang

import java.io._

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.sql.SQLContext
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
    val sqlContext = SQLContext.getOrCreate(sc)

    val modelFileIn = new File(args(0))
    val model = readModel(modelFileIn)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeClassificationModel]

    println(model)
    println(treeModel.toDebugString)
  }
}
