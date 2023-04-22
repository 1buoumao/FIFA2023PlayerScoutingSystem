package com.junlong.soccerplayerdata

import com.junlong.soccerplayerdata.ClusterSearch.spark
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{array_contains, col, explode, monotonically_increasing_id, split, when}

object PotentialModel {
  val FILE_PATH = "src/main/resources/male_players (legacy).csv"
  def ReadFile(): DataFrame = {

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FILE_PATH)

    val dfUpdated = df.withColumn("prediction", when(col("prediction") > 1.5, 0).otherwise(1))

    dfUpdated
      .select(
        col("player_id"),
        col("prediction"),
        col("short_name"),
        col("long_name"),
        col("player_positions"),
        col("overall"),
        col("potential"),
        col("value_eur"),
        col("wage_eur"),
        col("age"),
        col("club_name"),
        col("league_name"),
        col("nationality_name"))
  }

  def getPotential(id: Int):java.util.List[Row] = {
    val df = ReadFile()
    val res = df.filter(col("player_id")===id)
    val r = res.collectAsList()
    r
  }

}
