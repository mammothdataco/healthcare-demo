package com.mammothdata.demos

import com.redis._
import java.io.File
import com.mammothdata.nlp.Negex
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read, write}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint

object InfectionDemo extends java.io.Serializable {

  case class Result(patient_id: Integer, keyword: Boolean, bayes: Boolean, negex: Boolean)
  case class KError(keyword_error: Double, bayes_error: Double, negex_error: Double)
  case class RedisOutput(completed: Boolean, results: Array[Result], 
                         keyword_count: Long, affirmed_count: Long, 
                         bayes_count: Long, negex_count: Long, errors: KError)

  implicit val formats = Serialization.formats(NoTypeHints)

  val redis = new RedisClient(sys.env("REDIS_HOST"), 6379)
  val negexPath = "hdfs:///negex.txt"
  val bayesPath = "hdfs:///bayes"
  //var redis = new RedisClient("52.26.52.141", 6379)
  val driver = "com.mysql.jdbc.Driver"
  val conn = "jdbc:mysql://52.24.251.40:3306/infection?user=emr&password="+sys.env("DB_PASS")
  
  val options = Map[String, String]("driver" -> driver, "url" -> conn, "dbtable" -> "notes")
  val tf = new HashingTF(numFeatures = 1000)

  private def negexFilter(s: String, phrase: String, negex: Negex): Boolean = {
    negex.predict(s, phrase) match {
      case "Negated" => false
      case "Affirmed" => true
    }
  }

  private def bayesFilter(s: String, bayesModel: NaiveBayesModel): Boolean = {
    bayesModel.predict(tf.transform(s.sliding(2).toSeq)) == 0.0
  }

  private def collateResults(x:Integer,y:(Iterable[String], Iterable[String], Iterable[String])): Result = {
    new Result(x, y._1.nonEmpty, y._2.nonEmpty, y._3.nonEmpty)
  }
  
  private def nonMatching(x: Result):Boolean = x match {
    case Result(_, _, true, false) => true
    case _ => false
  }

  def runExperiment(df: DataFrame, bayesModel: NaiveBayesModel, negex: Negex, uuid: String): RDD[Result] = {
    val keywordR = df.rdd.map( x => (x(1), "keyword")) // use affirmed / negated
    val bayesR = df.rdd.filter(x => bayesFilter(x(2).toString,bayesModel)).map( x => (x(1), "bayes"))
    val affirmedR = df.filter("annotation = 'Affirmed'").rdd.map(x => (x(1), "affirmed" ))
    val negexR = df.rdd.filter(x => negexFilter(x(2).toString, x(3).toString, negex)).map( x => (x(1), "negex"))

    val joined = keywordR.cogroup(bayesR,negexR)

    val kerrors = collateErrors(affirmedR.count(), keywordR.count(), bayesR.count(), negexR.count())
    val comparisonResults = joined.map( x => collateResults((x._1).toString.toInt, x._2) )
    redis.set(uuid, write(new RedisOutput(true, comparisonResults.collect, 
              keywordR.count(), affirmedR.count(), bayesR.count(), negexR.count(), kerrors)))
    comparisonResults
  }

  private def collateErrors(totalAffirmed:Long, keywordCount: Long, bayesCount: Long, negexCount: Long):KError= {
    val keywordError = ((keywordCount - totalAffirmed).toDouble / totalAffirmed) * 100.0
    val bayesError = ((bayesCount - totalAffirmed).toDouble / totalAffirmed) * 100.0
    val negexError = ((negexCount - totalAffirmed).toDouble / totalAffirmed) * 100.0
    new KError(keywordError, bayesError, negexError)
  }


  def main(args: Array[String]) {
    val jars = Seq("hdfs:///infection2.jar")
    val conf = new SparkConf().setAppName("InfectionDemo").setJars(jars)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    var uuid = args(0)

    val bayesModel = NaiveBayesModel.load(sc, bayesPath)
    val negex = new Negex(sc.textFile(negexPath).collect, List("Negated", "Affirmed"))

    // Broadcast keywords (if using). In this demo, the keyword filter has already
    // been applied to the dataset.

    // val keywords = df.select("phrase").collect.map( _.toString ).map( _.replaceAll("\\]|\\[",""))
    // val bc = sc.broadcast(keywords)

    val df = sqlContext.load("jdbc", options);
    //val df = sqlContext.read.json("hdfs:///rescue.json")

    runExperiment(df, bayesModel, negex, uuid)
   
  }
}