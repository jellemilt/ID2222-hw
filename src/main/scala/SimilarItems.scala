import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{SortedSet, mutable}
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
    var intersect: Int = 0
    var union: Int = 0

    def compare_values(val1: Set[Int], val2: Set[Int], p1: Int, p2: Int): Double = {
      if (val1.isEmpty && val2.isEmpty )
        return union.toDouble/intersect.toDouble
      else if (val1.isEmpty || val2.isEmpty && val1(p1) > val2(p2))
        intersect += 1
      compare_values(val1, val2, p1, p2 +1)
      else if (val1.isEmpty || val2.isEmpty && val1(p1) < val2(p2))
        intersect += 1
      compare_values(val1, val2, p1 + 1, p2)
      else
      intersect += 1
      union += 1
      compare_values(val1, val2, p1 + 1, p2 +1)
    }

    val pos_doc1: Int = 0
    val pos_doc2: Int = 0

    compare_values(doc1, doc2, pos_doc1, pos_doc2)

  }

  def compareSets(A: Set[Int], B: Set[Int]): Double = {
    def compare(A: SortedSet[Int], B: SortedSet[Int], sizeIntersection: Int, sizeUnion: Int): Double = {
      if (A.isEmpty && B.isEmpty)
        sizeUnion.toDouble / sizeIntersection.toDouble
      else if (A.isEmpty || B.nonEmpty && A.head > B.head)
        compare(A, B.tail, sizeIntersection + 1, sizeUnion)
      else if (B.isEmpty || A.nonEmpty && A.head < B.head)
        compare(A.tail, B, sizeIntersection + 1, sizeUnion)
      else // A.head == B.head
        compare(A.tail, B.tail, sizeIntersection + 1, sizeUnion + 1)
    }

    compare(A.to[SortedSet], B.to[SortedSet], 0, 0)
  }




}
