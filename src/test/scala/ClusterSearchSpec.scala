import com.junlong.soccerplayerdata.ClusterSearch
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ClusterSearchSpec extends AnyFlatSpec with Matchers {
//  the jre should be 1.8 to avoid exception: cannot access to sno...
  val spark: SparkSession = SparkSession.builder
    .appName("Clustering")
    .master("local[*]")
    .getOrCreate()

  val id = 226766

  "FindCluster" should "work" in {
    ClusterSearch.FindCluster(id)
  }

  "findPos" should "work for 226766" in {
    val pos = ClusterSearch.FindPos(id)
    println(pos(0))
    pos.length should be(3)
  }

  "findPos" should "work for 158023" in {
    val pos = ClusterSearch.FindPos(158023)
    pos.length should be(1)
  }

  "ReadFile" should "work" in {
    val df = ClusterSearch.ReadFile()
//    df.describe().show()
    df.count() should be(18533)
  }

  "selectPos" should "work" in {
    val oridf = ClusterSearch.ReadFile()
    val df = ClusterSearch.selectPos("LW")
    df.count() should be(1169)
  }
}
