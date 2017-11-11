class CompareSignatures extends java.io.Serializable{

  def compareVectors(vec1: List[Int], vec2: List[Int]): Float ={
    val joinedVecs = vec1 zip vec2
    var sameCount=0f
    joinedVecs.foreach{case(v1,v2) => if(v1==v2){sameCount += 1f}}
    return sameCount/joinedVecs.size
  }
}
