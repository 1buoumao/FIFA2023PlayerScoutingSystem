package com.junlong.soccerplayerdata

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col, explode, monotonically_increasing_id, split}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans

object ClusterSearch {

  val spark = SparkSession.builder
    .appName("Clustering")
    .master("local[*]")
    .getOrCreate()

  val FILE_PATH = "/Users/junlongqiao/data/male_players (legacy).csv"


  def selectPos(position: String): DataFrame = {

    val ndf = ReadFile()

    val Players = ndf.filter(array_contains(split(col("player_positions"), ",\\s*"), position))

    Players

  }

  def Kmeans(dataFrame: DataFrame): DataFrame = {

    val rownum = dataFrame.count()


    val assembler = new VectorAssembler()
      .setInputCols(Array("pace", "shooting", "passing", "dribbling", "defending", "physic", "attacking_crossing", "attacking_finishing", "attacking_heading_accuracy", "attacking_short_passing", "attacking_volleys", "skill_dribbling", "skill_curve", "skill_fk_accuracy", "skill_long_passing", "skill_ball_control", "movement_acceleration", "movement_sprint_speed", "movement_agility", "movement_reactions", "movement_balance", "power_shot_power", "power_jumping", "power_stamina", "power_strength", "power_long_shots", "mentality_aggression", "mentality_interceptions", "mentality_positioning", "mentality_vision", "mentality_penalties", "mentality_composure", "defending_marking_awareness", "defending_standing_tackle", "defending_sliding_tackle", "goalkeeping_diving", "goalkeeping_handling", "goalkeeping_kicking", "goalkeeping_positioning", "goalkeeping_reflexes"))
      .setOutputCol("features")

    val vectorData = assembler.transform(dataFrame).select("features")


    val kmeans = new KMeans()
      .setK(rownum.toInt / 10) // 设置簇数
      .setSeed(1) // 设置随机种子
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(vectorData)

    val predictions = model.transform(vectorData)



    val stWithIndex = dataFrame.withColumn("index", monotonically_increasing_id())
    val pdWithIndex = predictions.withColumn("index", monotonically_increasing_id())

    val joined = stWithIndex.join(pdWithIndex, Seq("index")).drop("index").drop("features")

    joined

  }

  def FindCluster(id: Int): java.util.List[Row]= {

    val pos = FindPos(id)

    val ori_df = ReadFile()

    val df = selectPos(pos(0))

    var c = Kmeans(df)

    var targetCluster = c.filter(col("player_id") === id).select(col("cluster")).first.getInt(0)

    var result = c.filter(col("cluster") === targetCluster).drop("cluster")




    for (elem <- pos.tail) {

      val df = selectPos(elem)

      c = Kmeans(df)

      targetCluster = c.filter(col("player_id") === id).select(col("cluster")).first.getInt(0)

      result = result.union(c.filter(col("cluster") === targetCluster).drop("cluster"))

    }

    //result.distinct()

    val a = result.distinct().select(
      col("player_id"),
      col("short_name"),
      col("player_positions"),
      col("overall"),
      col("potential"),
      col("value_eur"),
      col("wage_eur"),
      col("age"),
      col("club_name")
      )

    val list = a.collectAsList()

    list

  }


  def FindPos(id: Int): Array[String] = {
    val df = ReadFile()
    val pos = df.filter(col("player_id") === id).select("player_positions").head().getString(0)
    val positionArr = pos.split(", ")
    positionArr
  }



  def ReadFile(): DataFrame = {

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FILE_PATH)


    val ndf = df.filter(col("fifa_version") === 23)

    ndf

  }



  def main(args: Array[String]): Unit = {
  }

}
