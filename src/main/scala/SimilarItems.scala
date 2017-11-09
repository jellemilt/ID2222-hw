import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedSet
import scala.util.Random

object SimilarItems {
  def main(args: Array[String]) {
    // Set up Spark
    val conf = new SparkConf()
      .setAppName("Similar Items")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filePath = "src/Data/Part1/awards_1990/awd_1990_00/"

    val docs = sc.wholeTextFiles(filePath).cache()
      .mapValues(text => text.replace('\n', ' ').replace("  ", " ")) //replace line breaks with space
      .map({case (path, text) => (path.split("/").last, text)}) //store only name of the document, instead of the whole path

    docs.take(5).foreach(println)

    val shingles = docs.mapValues(makeShingles(_, 10))
    shingles.take(5).foreach(println)

  }

  def makeShingles(document: String, k: Int): Set[String] = {
    (0 until k)
      .flatMap(document.substring(_).grouped(k))
      .filter(_.length == k)
      .toSet
  }

}
