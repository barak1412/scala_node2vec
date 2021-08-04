package org.demo
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

object Demo {
  def main(args: Array[String]): Unit = {
    // create session
    val spark = SparkSession.builder()
      .appName("BarakApp")
      .master(master = "local[*]")
      .getOrCreate()
    import spark.implicits._

    val srcCol: String = "SRC"
    val dstCol: String = "DST"
    val weightCol: String = "WEIGHT"
    val df = Seq(("V1", "V2", 2.0f), ("V2", "V1", 2.0f), ("V1", "V3", 3.0f), ("V3", "V1", 3.0f))
      .toDF(srcCol, dstCol, weightCol)

    val n: Node2Vec = new Node2Vec(srcCol = srcCol, dstCol = dstCol, weightCol = weightCol, walkLength = 10)
    val walks = n.fit(df).persist()
    walks.show()

    val word2Vec = new Word2Vec()
      .setInputCol("WALKS")
      .setOutputCol("VECTOR")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(walks)

    val result = model.getVectors
    result.show()
    walks.unpersist()
    spark.stop()
    println("Done.")
  }
}
