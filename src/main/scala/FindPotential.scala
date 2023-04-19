import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._

object FindPotential {

  def getTrainData(oridf: DataFrame, spark: SparkSession):DataFrame = {
    val df = oridf.selectExpr("player_id", "fifa_version", "player_positions", "cast(overall as double) overall", "cast(potential as double) potential", "cast(value_eur as double) value_eur", "cast(age as double) age", "cast(height_cm as double) height_cm", "cast(weight_kg as double) weight_kg", "cast(league_level as double) league_level", "club_joined_date", "club_contract_valid_until_year")

    //    df.describe().show()
    val df2022 = df.filter(col("fifa_version") === 22)
    val df2023 = df.filter(col("fifa_version") === 23)
    //    df2023.describe().show()
    //    df2022.describe().show()

    df2022.createOrReplaceTempView("table22")
    df2023.createOrReplaceTempView("table23")

    val commonDF22: DataFrame = spark.sql("select * from table22 where player_id in (SELECT table22.player_id FROM table22, table23 where table22.player_id = table23.player_id)")

    //    commonDF22.describe().show()
    //    commonDF23.describe().show()

    // max level is 5 so fill with 6
    val filled22DF = commonDF22.na.fill(0, Seq("league_level")).na.fill(6)
    filled22DF
  }

  def addShortName(spark: SparkSession, oridf: DataFrame, predictions23: DataFrame): DataFrame = {
    val nameDF = oridf
      .selectExpr("player_id", "short_name")
    predictions23.createOrReplaceTempView("tablePrediction")
    nameDF.createOrReplaceTempView("tableName")
    val resDF: DataFrame = spark.sql("select distinct * from tablePrediction inner join tableName on tablePrediction.player_id = tableName.player_id")
    //    resDF.describe().show()
    resDF
  }

  def findPotential(): DataFrame = {
    val spark = SparkSession.builder()
      .appName("Player Prediction")
      .master("local[*]")
      .getOrCreate()

    val oridf = spark.read.format("csv")
      .option("header", "true")
      .load("src/main/resources/male_players (legacy).csv")

    val filled22DF = getTrainData(oridf, spark)

    // training
    val indexer = new StringIndexer()
      .setInputCols(Array("player_positions"))
      .setOutputCols(Array("player_positions_idx"))

    val assembler = new VectorAssembler()
      .setInputCols(Array("overall", "potential", "value_eur", "age", "height_cm", "weight_kg", "league_level")) //, "player_positions_idx"
      .setOutputCol("features")

    val transformed22DF = indexer.fit(filled22DF).transform(filled22DF)
    val feature22DF = assembler.transform(transformed22DF)

    val rf = new RandomForestRegressor()
      .setLabelCol("league_level")
      .setFeaturesCol("features")
      .setNumTrees(10)

    val model = rf.fit(feature22DF)
    val predictions22 = model.transform(feature22DF)

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("league_level")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    val auc = evaluator.evaluate(predictions22)

    println(s"AUC = $auc")

    // test

    val commonDF23: DataFrame = oridf
      .selectExpr("player_id", "fifa_version", "player_positions", "cast(overall as double) overall", "cast(potential as double) potential", "cast(value_eur as double) value_eur", "cast(age as double) age", "cast(height_cm as double) height_cm", "cast(weight_kg as double) weight_kg", "cast(league_level as double) league_level", "club_joined_date", "club_contract_valid_until_year")
      .filter(col("fifa_version") === 23)
    val filled23DF = commonDF23.na.fill(0, Seq("league_level")).na.fill(0)
    // Transform the input data into a feature vector using the trained StringIndexer and VectorAssembler
    val transformed23DF = indexer.fit(filled23DF).transform(filled23DF)
    val feature23DF = assembler.transform(transformed23DF)
    val predictions23 = model.transform(feature23DF)

//    predictions23.describe().show()

    val resDF = addShortName(spark, oridf, predictions23);
    resDF
  }

  def findPotentialByShortName(name: String, df: DataFrame): Boolean = {
    val selectedDF = df.filter(col("short_name").equalTo(name))
//    selectedDF.describe().show()
    val level = selectedDF.first().getAs[Double]("prediction")
    if (level<1.5) {
      true
    } else {false}
  }

  def findPotentialByPlayerId(id: Integer, df: DataFrame): Boolean = {
    val selectedDF = df.filter(col("tableprediction.player_id").equalTo(id))
    selectedDF.describe().show()
    val level = selectedDF.first().getAs[Double]("prediction")
    if (level < 1.5) {
      true
    } else {
      false
    }
  }
}
