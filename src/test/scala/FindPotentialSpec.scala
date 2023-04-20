import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class FindPotentialSpec extends AnyFlatSpec with Matchers {
  // the jre should be 1.8 to avoid exception: cannot access to sno...
  val spark: SparkSession = SparkSession.builder()
    .appName("Player Prediction")
    .master("local[*]")
    .getOrCreate()

  "findPotentialDF" should "work" in {
    val predictionDF = FindPotential.findPotentialDF()
  }

  "getTrainData" should "have right rows" in {
    val oridf: DataFrame = spark.read.format("csv")
      .option("header", "true")
      .load("src/main/resources/male_players (legacy).csv")
    val filled22DF = FindPotential.getTrainData(oridf, spark)
    filled22DF.describe().show()
    filled22DF.count() should be(13011)
  }
}
