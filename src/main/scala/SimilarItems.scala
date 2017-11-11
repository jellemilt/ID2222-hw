import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.SortedSet
import scala.util.Random


object SimilarItems {
  def main(args: Array[String]) {
    // Set up Spark
    val conf = new SparkConf()
      .setAppName("Similar Items")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filePath = "src/Data"//Part1/awards_1990/awd_1990_00/"

    val docs = sc.wholeTextFiles(filePath).cache()
      .mapValues(text => text.replace("\\s+", " ")) //replace line breaks with space
      .map({case (path, text) => (path.split("/").last, text)}) //store only name of the document

    //make shingles and hash the shingles to a Int value
    val hashedShingles = docs.mapValues(makeShingles(_, 5))
      .mapValues(_.map(_.hashCode.intValue()).to[SortedSet]) // Hash the shingles to hashCode and sort the list

    //Use minHasher to turn hashedShingles into a signature
    val minHasher = new MinHashing(1000000000,1000)
    val minHashedShingles = hashedShingles.mapValues{hashedShingle => minHasher.minHash(hashedShingle.toSet)}
    //val keys = minHashedShingles.collectAsMap().keySet

    //Use Locality Sensitive Hashing to dertermine candidate pairs of document, based on their signature
    val candidateDocuments = LSH(minHashedShingles)


    //***** Compare documents *****

    val compareSignatures = new CompareSignatures
    val t = 0.85 // threshold of similarity

    //compare candidate documents by their signatures
    val comparedSignatures = candidateDocuments.map({case (doc1, doc2) => (doc1, doc2, signatureSimilarity(doc1._2, doc2._2))})

    //keep only the pairs whose similarity is above the threshold
    val similarDocumentsWRTSignature = comparedSignatures
      .filter({case (doc1, doc2, score) => score >= t})

    //Compare documents based on their shingles
    val comparedShingles = similarDocumentsWRTSignature.map({case (doc1,doc2,score) =>
      val jaccardScore = jaccardSimilarity(doc1._2,doc2._2)
      (doc1,doc2,jaccardScore)})

    //Keep only the paris whose similarity is above threshold
    val similarDocumentsWRTShingles = comparedShingles
      .filter({case (doc1, doc2, score)=> score >= t})

//    val prettifiedSimilarItems = similarDocumentsWRTShingles
//      .map({case (doc1, doc2, score) => (doc1._1, doc2._1, score)})


    //***** Print results *****

    println("Resulting LSH candidate pairs, filtered by their signature comparison")
    similarDocumentsWRTSignature.collect().foreach({case(doc1,doc2,score)=> println("%s and %s | %.4f".format(doc1._1, doc2._1, score))})

    println("LSH candidate pairs, filtered by signature comparison, and by the shingle comparison")
    similarDocumentsWRTShingles.collect().foreach({case(doc1,doc2,score)=> println("%s and %s | %.4f".format(doc1._1, doc2._1, score))})

    println("MinHashed documents filtered by signature comparison")
    //Creating a copy of minHashedShingles to distribute among RDD nodes
    val mHSnoSpark = minHashedShingles.collectAsMap()
    minHashedShingles.foreach{
        case(key1,mhs1)=>
          mHSnoSpark.foreach{
            case(key2,mhs2)=>{
              val similarity = signatureSimilarity(mhs1,mhs2)
              if (similarity >0.3f && similarity < 1)
                println(key1+" and "+key2+" -> MinHash Signature Similarity: "+similarity)
            }}}


    }

  //******* Functions *******


  // 1.) A function that creates a set of shingles out of an string input
  def makeShingles(document: String, k: Int): Set[String] = {
    (0 until k)
      .flatMap(document.substring(_).grouped(k))
      .filter(_.length == k)
      .toSet
  }
  // 2.) A function that calculates the similarity of two documents based on their hashed shingles
  def jaccardSimilarity(doc1: List[Int], doc2: List[Int]): Double = {
    val (sorteddoc1, sorteddoc2) = (doc1.to[SortedSet], doc2.to[SortedSet])
    val score = sorteddoc1.intersect(sorteddoc2).size.toDouble/(sorteddoc1 ++ sorteddoc2).size.toDouble
    score
  }

  // 3.) A Function that creates a signature out of a list of hashed shingles
  //THIS DOES NOT WORK, since the hashes have to be static
  def minHashing(shingle: Set[Int], range: Int, hashAmount: Int): Set[Int] = {
    //defines how many possibilities there are for hash functions

    val randomness = Int.MaxValue
    //Set min value, so that all bits in the integer are potentially set
    val minHashValue = 10000000
    //start by defining -hashAmount- different hashs of the form ((s*m)^t)%range
    var hashes: List[(Int,Int)] = List.fill(hashAmount)(Random.nextInt(randomness),Random.nextInt(randomness))
    val minHashs = hashes.map{case(m,t) => shingle.map(s => Math.abs((s*m)^t)%range).min}
    //No modulo needed. It would have to be Modulo 2^32 which isulos  the size of Int -> Int modit by default
    minHashs.toSet
  }
  // 4.) A function that calculates the similarity of two documents based on their signatures
  def signatureSimilarity(sign1: List[Int], sign2: List[Int]): Double = {
    val joinedVecs = sign1 zip sign2
    var sameCount=0f
    joinedVecs.foreach{case(v1,v2) => if(v1==v2){sameCount += 1f}}
    sameCount/joinedVecs.size
    //sign1.zip(sign2).count({ case (a, b) => a == b }).toDouble / sign1.size.toDouble
  }

  // 5.) A function that calculates candidate pairs that might be similar, based on bands of their signatures
  def LSH(docs: RDD[(String, List[Int])]): RDD[((String, List[Int]), (String, List[Int]))] = {

    val numberOfBuckets = 1000000 //number of buckets
    val bandSize = 5

    //make buckets and hash all bands of all documents in the different buckets
    val candidatePairs = docs.flatMap({ case (name, sign) =>
        sign.grouped(bandSize).map(piece => {
          val bucket = piece.hashCode%numberOfBuckets
          (bucket, (name, sign))})})

    //Get all pairs of document of which at least one band falls in the same bucket

      .groupByKey() //group by bucket
      .flatMap({case (bucket, listOfDocs) => listOfDocs.cross(listOfDocs)}) // create a cartesian product of all docs in
      //a bucket
      .map({case (doc1, doc2) => if (doc1._1 < doc2._1)(doc1, doc2) else (doc2, doc1)}) //make sure that all pairs are
      //similarly ordered
      .filter({case (doc1, doc2) => doc1 != doc2}) //keep only the pairs that are not identical
      .distinct()//remove duplicates


    candidatePairs

  }

// do cross product (src: https://stackoverflow.com/questions/14740199/cross-product-in-scala)
  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

}
