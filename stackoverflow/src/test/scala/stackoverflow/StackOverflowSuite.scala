package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.net.URL
import java.nio.channels.Channels
import java.io.File
import java.io.FileOutputStream

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }
  lazy val sc = StackOverflow.sc

  lazy val postings = sc.parallelize(List(
    Posting(1, 1, None, None, 1, None),
    Posting(2, 2, None, Some(1), 100, None),
    Posting(2, 3, None, Some(1), 10, None),
    Posting(1, 4, None, None, 1, None),
    Posting(2, 5, None, Some(4), 4, None),
    Posting(1, 6, None, None, 1, None)
  ))

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("grouped postings") {

    val grouped: Array[(Int, Iterable[(Posting, Posting)])] = testObject.groupedPostings(postings).collect()
    grouped.foreach(println)
    assert(grouped.length == 2)
    assert(grouped.filter(_._1 == 1).flatMap(_._2).length == 2)
    assert(grouped.filter(_._1 == 4).flatMap(_._2).length == 1)
    assert(grouped.filter(_._1 == 6).flatMap(_._2).length == 0)

  }

  test("compute score") {
    val grouped = testObject.groupedPostings(postings)
    val scores = testObject.scoredPostings(grouped).collect()
    assert(scores.filter(_._1.id == 1).head._2 == 100)
    assert(scores.filter(_._1.id == 4).head._2 == 4)
  }

  test("vector posting") {
    val scores =
      sc.parallelize(List((Posting(1,6,None,None,140,Some("CSS")),67),
      (Posting(1,42,None,None,155,Some("PHP")),89),
      (Posting(1,72,None,None,16,Some("Ruby")),3),
      (Posting(1,126,None,None,33,Some("Java")),30),
      (Posting(1,174,None,None,38,Some("C#")),20)))
    val vectorPostings = testObject.vectorPostings(scores).collect()
    val expected = List((350000,67), (100000,89), (300000,3), (50000,30), (200000,20))
    expected.foreach(score => {
      assert(vectorPostings.contains(score))
    })
  }

}
