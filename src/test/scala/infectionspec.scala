import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfter
import com.redis._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import scala.io.Source


import com.mammothdata.nlp._
import com.mammothdata.demos.InfectionDemo

// Despite what the Spark documentation tells you, this is really an
// integration test rather than a unit test. As you'll have to run Spark,
// you'll also have to set up a local Redis instance for these tests
// to pass

class InfectionSpec extends FlatSpec with BeforeAndAfter {

  private val master = "local[2]"
  private val appName = "InfectionSpec"

  private var sc: SparkContext = _
  private var sqlContext: SQLContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
    sqlContext = new SQLContext(sc)
    InfectionDemo.redis = new RedisClient("localhost", 6379)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }

  def fixture = new {
    val testData = sqlContext.read.json("test.json")
  }

  private def nonMatching(x: InfectionDemo.Result):Int = x match {
    case InfectionDemo.Result(_, _, _, true) => 1
    case _ => 0
  }

  "InfectionDemo" should "run the experiment" in {
    val bayesModel = NaiveBayesModel.load(sc, "bayes")
    val negex = new Negex(Source.fromFile("negex.txt").getLines().toArray, List("Negated", "Affirmed"))
    val results = InfectionDemo.runExperiment(fixture.testData, bayesModel, negex, "test").collect()
    assert(results.map(nonMatching(_)).reduce(_+_) == 2)
  }
    
}