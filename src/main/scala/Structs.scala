object Structs {

  case class ClusterIndex(cluster:Int,sameCluster:Seq[(Double,Double)])

  case class dataWithClusters(cordinates:(Double,Double),cluster:Int)

  case class SumDistance(cordinates:(Double,Double), sum:Double)

  case class ClusterDistances(cluster:Int,distances:Seq[SumDistance],mean:Double,stdDev:Double)
}
