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
      .mapValues(text => text.replace("\\s+", " ")) //replace line breaks with space
      .map({case (path, text) => (path.split("/").last, text)}) //store only name of the document, instead of the
    // whole path

    //docs.take(5).foreach(println)

    //make shingles and hash the shingles to a Int value
    val hashedShingles = docs.mapValues(makeShingles(_, 3))
      .mapValues(_.map(_.hashCode.intValue()).to[SortedSet]) // Hash the shingles to
      // hashCode and sort the list
      .collectAsMap()


    //*************************************************************
    //THIS IS WHERE MY PART STARTS
    //YOU CAN CHANGE THE REST TO WHATEVER THE FUCK
    //IF THE CODE DOWN HERE DOESNT WORK THEN; JUST COMMENT IT
    //ILL FIX IT THEN AFTERWARDS
    //*************************************************************
    val minHasher = new MinHashing(1000000000,100)
    val minHashedShingles = hashedShingles.mapValues{hashedShingle => minHasher.minHash(hashedShingle.toSet)}

    val keys = minHashedShingles.keySet
    hashedShingles.take(2).foreach(println)
    //minHashedShingles.take(2).foreach(println)
    //println((minHashedShingles apply keys.head)) //(a9001001.txt,TreeSet(-2145272195, -2142988454, -2140567169, ..))
    val compareSignatures = new CompareSignatures
    println((minHashedShingles apply keys.head).size)
    println(keys.getClass)
    minHashedShingles.foreach{
        case(key1,mhs1)=>
          minHashedShingles.foreach{
            case(key2,mhs2)=>{
              val similarity = compareSignatures.compareVectors(mhs1,mhs2)
              if (similarity >0.6f && similarity < 1)
                println(key1+" and "+key2+" -> "+similarity)
            }}}


  }

  // A function that creates a set of shingles out of an string input
  def makeShingles(document: String, k: Int): Set[String] = {
    (0 until k)
      .flatMap(document.substring(_).grouped(k))
      .filter(_.length == k).distinct
      .toSet
  }

  def minHashing(shingle: Set[Int], hashAmount: Int): Set[Int] = {
    //defines how many possibilities there are for hash functions
    val randomness = 10000
    //start by defining -hashAmount- different hashs of the form y=mx+t
    val hashs = List.fill(hashAmount)(Random.nextInt(randomness),Random.nextInt(randomness))
    val minHashs = hashs.map{case(m,t) => shingle.map(m*_+t).min}
    //No modulo needed. It would have to be Modulo 2^32 which is the size of Int -> Int modulos it by default
    return minHashs.toSet
  }

  // A function that calculates the Jaccard Similarity out of two Sets of Integers
  def calculateJaccardSimilarity(doc1: Set[Int], doc2: Set[Int]): Double = {
    var placeholer: Double = 0.5
    return placeholer
  }




}
