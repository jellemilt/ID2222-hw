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
    val filePath = "src/Data/Part1/awards_1990/awd_1990_01/"

    val docs = sc.wholeTextFiles(filePath).cache()
      .mapValues(text => text.replace('\n', ' ').replace("  ", " ")) //replace line breaks with space
      .map({case (path, text) => (path.split("/").last, text)}) //store only name of the document, instead of the
    // whole path

    //docs.take(5).foreach(println)

    //make shingles and hash the shingles to a Int value
    val hashedShingles = docs.mapValues(makeShingles(_, 10))
      .mapValues(setx => setx.map(shingle => shingle.hashCode.intValue()).to[SortedSet]) // Hash the shingles to
      // hashCode and sort the list
      .collectAsMap()
    hashedShingles.take(1).foreach(println) //(a9001001.txt,TreeSet(-2145272195, -2142988454, -2140567169, ..))


  }

  // A class that creates a set of shingles out of an string input
  def makeShingles(document: String, k: Int): Set[String] = {
    (0 until k)
      .flatMap(document.substring(_).grouped(k))
      .filter(_.length == k)
      .toSet
  }

  // A class that calculates the Jaccard Similarity out of two Sets of Integers
  def calculateJaccardSimilarity(doc1: Set[Int], doc2: Set[Int]): Double = {
    var placeholer: Double = 0.5
    return placeholer
  }




}
