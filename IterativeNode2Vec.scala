package org.demo

import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{array, col, collect_list, explode, lit, struct, udf}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.{ListBuffer, WrappedArray}
import scala.util.Random

class IterativeNode2Vec (srcCol: String, dstCol: String, weightCol: String, neighborsLimit: Int = 10,
                         p: Float = 1.0f, q: Float = 1.0f, walkLength: Int = 10, numOfWalks: Int = 5){

  private var aliasNodesDf: Option[DataFrame] = None
  private var aliasEdgesDf: Option[DataFrame] = None

  def setupProbabilities(df: DataFrame): Unit = {
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

    aliasNodesDf =
      Some(neighborsGroupedDf.withColumn("ALIAS_NODE", aliasNodeUdf(col("NEIGHBORS_LIST")))
        .select(srcCol, "ALIAS_NODE").persist())

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

    aliasEdgesDf =
      Some(neighborsDf.join(neighborsGroupedDf.selectExpr(s"$srcCol AS $dstCol", "NEIGHBORS_LIST"), Seq(dstCol), "inner")
        .selectExpr(srcCol, dstCol, "NEIGHBORS_LIST AS DST_NEIGHBORS_LIST")
        .join(neighborsGroupedDf, Seq(srcCol), "inner")
        .selectExpr(srcCol, dstCol, "NEIGHBORS_LIST AS SRC_NEIGHBORS_LIST", "DST_NEIGHBORS_LIST")
        .withColumn("ALIAS_EDGE",
          aliasEdgeUdf(col(srcCol), col(dstCol), col("SRC_NEIGHBORS_LIST"), col("DST_NEIGHBORS_LIST")))
        .select(srcCol, dstCol, "ALIAS_EDGE").persist())


    // clean cached memory
    neighborsDf.unpersist(blocking = false)
    neighborsGroupedDf.unpersist(blocking = false)
  }

  //def fit(df: DataFrame): RDD[Array[Array[String]]] = {
  def fit(df: DataFrame): DataFrame = {
    // retrieve current running spark session
    val spark: SparkSession = SparkSession.getActiveSession.get
    val sc: SparkContext = spark.sparkContext

    // setup transaction probabilities for nodes and edges
    this.setupProbabilities(df)

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
    val aliasDrawUdf = udf(aliasDraw)
    val makeSingletonListUdf = udf((s: String) => Seq[String](s))
    val insertToListUdf = udf((l: Seq[String], e: String) => l :+ e)
    var c: Long = 0

    var generatedWalks = df.select(srcCol).distinct()
      .withColumn("DUMMY", explode(array((0 until numOfWalks).map(lit): _*)))
      .withColumn("WALKS", makeSingletonListUdf(col(srcCol)))
      .drop("DUMMY")

    for (i <- 2 until(walkLength + 1)){
      if (i == 2){
        generatedWalks = generatedWalks.join(aliasNodesDf.get, Seq(srcCol), "inner")
          .withColumn("CHOSEN_NODE", aliasDrawUdf(col("ALIAS_NODE")))
          .selectExpr("WALKS", s"$srcCol AS PREV_NODE", "CHOSEN_NODE")
          .withColumn("WALKS", insertToListUdf(col("WALKS"), col("CHOSEN_NODE")))
          .repartition(col("CHOSEN_NODE"))
          .persist()
        generatedWalks.count()
      }
      else{
        generatedWalks = generatedWalks.selectExpr("WALKS", s"PREV_NODE as $srcCol", s"CHOSEN_NODE as $dstCol")
          .join(aliasEdgesDf.get, Seq(srcCol, dstCol), "inner")
          .withColumn("CHOSEN_NODE", aliasDrawUdf(col("ALIAS_EDGE")))
          .selectExpr("WALKS", s"$dstCol AS PREV_NODE", "CHOSEN_NODE")
          .withColumn("WALKS", insertToListUdf(col("WALKS"), col("CHOSEN_NODE")))
          .repartition(col("CHOSEN_NODE"))
          .persist()
        generatedWalks.count()
      }
    }

    generatedWalks.select("WALKS")
  }
}
