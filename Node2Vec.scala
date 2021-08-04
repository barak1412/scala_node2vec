package org.demo

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray

import scala.collection.Map
import scala.collection.mutable.ListBuffer
import scala.util.Random

class Node2Vec(srcCol: String, dstCol: String, weightCol: String, neighborsLimit: Int = 10,
               p: Float = 1.0f, q: Float = 1.0f, walkLength: Int = 10, numOfWalks: Int = 5){

  def setupProbabilities(df: DataFrame):
    (Map[String, WrappedArray[Row]], Map[(String, String), WrappedArray[Row]]) = {
    val aliasSetup = (probs: Array[Float]) => {
      val qList: Array[Float] = Array.fill(probs.length)(0.0f)
      val jList: Array[Int] = Array.fill(probs.length)(0)
      val smaller: ListBuffer[Int] = ListBuffer[Int]()
      val larger: ListBuffer[Int] = ListBuffer[Int]()
      var small: Int = 0
      var large: Int = 0

      for(i <- probs.indices){
        qList(i) = probs(i) * probs.length
        // push index to the beginning of the list
        if (qList(i) < 1.0f)
          i+=: smaller
        else
          i+=: larger
      }

      while (smaller.nonEmpty && larger.nonEmpty){
        small = smaller.remove(0)
        large = larger.remove(0)

        jList(small) = large
        qList(large) = qList(large) + qList(small) - 1.0f
        if (qList(large) < 1.0f)
          large +=: smaller
        else
          large +=: larger
      }

      (jList, qList)
    }

    val aliasNode = (neighborsList: Array[(String, Float)]) => {
      val weightsSum: Float = neighborsList.map(e => e._2).sum
      val jqArrays: (Array[Int], Array[Float]) = aliasSetup(neighborsList.map(e => e._2 / weightsSum))
      val jqNeighbors: Array[(String, Int, Float)] = new Array[(String, Int, Float)](neighborsList.length)
      var i: Int = 0
      while (i < neighborsList.length){
        jqNeighbors(i) = (neighborsList(i)._1, jqArrays._1(i), jqArrays._2(i))
        i += 1
      }

      jqNeighbors
    }
    val aliasNodeUdf = udf(aliasNode)

    val neighborsDf = df
      .selectExpr("*", s"RANK() OVER(PARTITION BY $srcCol ORDER BY $weightCol DESC) AS NEIGHBOR_RANK")
      .where(s"NEIGHBOR_RANK <= $neighborsLimit")
      .drop("NEIGHBOR_RANK")
      .persist()
    val neighborsGroupedDf = neighborsDf
      .groupBy(srcCol).agg(collect_list(struct(dstCol, weightCol)).alias("NEIGHBORS_LIST"))
      .persist()

    val aliasNodes: Map[String, WrappedArray[Row]] =
      neighborsGroupedDf.withColumn("ALIAS_NODE", aliasNodeUdf(col("NEIGHBORS_LIST")))
        .select(srcCol, "ALIAS_NODE")
        .rdd
        .map{case Row(srcNode: String, neighborsJQ: WrappedArray[Row]) => (srcNode, neighborsJQ)}
        .collectAsMap()

    val pValue = p
    val qValue = q
    val aliasEdge = (src: String, dst: String, srcNeighborsList: Array[(String, Float)], dstNeighborsList: Array[(String, Float)]) => {
      val unNormalizedProbs = dstNeighborsList.map(e =>
        if (src == e._1)
          e._2 / pValue
        else if (srcNeighborsList.exists(n => n._1 == e._1))
          e._2
        else
          e._2 / qValue
      )
      val weightsSum: Float = unNormalizedProbs.sum
      val jqArrays: (Array[Int], Array[Float]) = aliasSetup(unNormalizedProbs.map(e => e / weightsSum))
      val jqNeighbors: Array[(String, Int, Float)] = new Array[(String, Int, Float)](dstNeighborsList.length)
      var i: Int = 0
      while (i < dstNeighborsList.length){
        jqNeighbors(i) = (dstNeighborsList(i)._1, jqArrays._1(i), jqArrays._2(i))
        i += 1
      }

      jqNeighbors
    }
    val aliasEdgeUdf = udf(aliasEdge)

    val aliasEdges: Map[(String, String), WrappedArray[Row]] =
      neighborsDf.join(neighborsGroupedDf.selectExpr(s"$srcCol AS $dstCol", "NEIGHBORS_LIST"), Seq(dstCol), "inner")
      .selectExpr(srcCol, dstCol, "NEIGHBORS_LIST AS DST_NEIGHBORS_LIST")
      .join(neighborsGroupedDf, Seq(srcCol), "inner")
      .selectExpr(srcCol, dstCol, "NEIGHBORS_LIST AS SRC_NEIGHBORS_LIST", "DST_NEIGHBORS_LIST")
      .withColumn("ALIAS_EDGE",
                aliasEdgeUdf(col(srcCol), col(dstCol), col("SRC_NEIGHBORS_LIST"), col("DST_NEIGHBORS_LIST")))
      .select(srcCol, dstCol, "ALIAS_EDGE")
      .rdd
      .map{case Row(srcNode: String, dstNode: String, neighborsJQ: WrappedArray[Row]) =>
          (Tuple2(srcNode, dstNode), neighborsJQ)}
      .collectAsMap()

    // clean cached memory
    neighborsDf.unpersist(blocking = false)
    neighborsGroupedDf.unpersist(blocking = false)

    // return both alias nodes and edges
    (aliasNodes, aliasEdges)
  }

  //def fit(df: DataFrame): RDD[Array[Array[String]]] = {
  def fit(df: DataFrame): DataFrame = {
    // retrieve current running spark session
    val spark: SparkSession = SparkSession.getActiveSession.get
    val sc: SparkContext = spark.sparkContext

    // setup transaction probabilities for nodes and edges
    val aliases = this.setupProbabilities(df)

    // generate walks
    val aliasDraw = (neighborsJQ: WrappedArray[Row]) => {
      val kk: Int = (Random.nextFloat() * neighborsJQ.length).toInt
      var chosenIndex: Int = 0
      if (Random.nextFloat() < neighborsJQ(kk).getAs[Float](2))
        chosenIndex = kk
      else
        chosenIndex = neighborsJQ(kk).getAs[Int](1)
      neighborsJQ(chosenIndex).getAs[String](0)
    }

    val broadcastAliasNodes = sc.broadcast(aliases._1)
    val broadcastAliasEdges = sc.broadcast(aliases._2)
    val walkLengthValue = walkLength
    val numOfWalksValue = numOfWalks
    val generateWalks = (node: String) => {
      val walks: Array[Array[String]] = new Array[Array[String]](numOfWalksValue)
      for (i <- 0 until numOfWalksValue){
        walks(i) = new Array[String](walkLengthValue)
        for (j <- 0 until walkLengthValue){
          if (j == 0){
            walks(i)(j) = node
          }
          else if (j == 1){
            // use alias node
            walks(i)(j) = aliasDraw(broadcastAliasNodes.value(node))
          }
          else{
            // use alias edge
            walks(i)(j) = aliasDraw(broadcastAliasEdges.value((walks(i)(j-2), walks(i)(j-1))))
          }
        }
      }
      walks
    }

    val generateWalksUdf = udf(generateWalks)
    val generatedWalks = df.select(srcCol).distinct()
      .withColumn("WALKS", generateWalksUdf(col(srcCol)))
      .withColumn("WALKS", explode(col("WALKS")))
      .select("WALKS")

    generatedWalks
  }
}
