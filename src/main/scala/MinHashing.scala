import org.spark_project.guava.primitives.UnsignedInteger

import scala.util.Random
//hashAmount -> how many hashes
//randomness -> how many possibilities for hashes -> there are pow(randomness,2) possible hashes
class MinHashing(range: Int, hashAmount: Int) extends java.io.Serializable {

  val randomness = Int.MaxValue
  //Min value to
  val minHashValue = 10000000
  //start by defining -hashAmount- different hashs of the form ((s*m)^t)%range
  var hashes: List[(Int,Int)] = List.fill(hashAmount)(Random.nextInt(randomness),Random.nextInt(randomness))

  def minHash(shingle: Set[Int]): List[Int] = {
    hashes.map{case(m,t) => shingle.map(s => Math.abs((s*m)^t)%range).min}
  }

}
