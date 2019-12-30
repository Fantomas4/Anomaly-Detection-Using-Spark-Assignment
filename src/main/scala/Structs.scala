object Structs {

  case class ClusterIndex(cluster:Int,sameCluster:Seq[(Double,Double)],center:(Double,Double))

  case class dataWithClusters(cordinates:(Double,Double),cluster:Int)

  case class Distance(cordinates:(Double,Double), distance:Double)

  case class ClusterDistances(cluster:Int,distances:Seq[Distance],mean:Double,stdDev:Double)
}
