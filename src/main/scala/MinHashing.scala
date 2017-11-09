import org.spark_project.guava.primitives.UnsignedInteger

import scala.util.Random
//hashAmount -> how many hashes
//randomness -> how many possibilities for hashes -> there are pow(randomness,2) possible hashes
class MinHashing(randomness: Int, hashAmount: Int) extends java.io.Serializable {

  //start by defining -hashAmount- different hashs of the form y=mx+t
  var hashes: List[(Int,Int)] = List.fill(hashAmount)(Random.nextInt(randomness),Random.nextInt(randomness))

  def minHash(shingle: Set[Int]): Set[Int] = hashes.map{case(m,t) => shingle.map(s => Math.abs(m*s+t)).min}.toSet

}
