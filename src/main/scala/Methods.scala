import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

import scala.collection.mutable


object Methods {

  /**
   * Calculates the euclidean distance between 2d points
   *
   * @param x1 x from first point
   * @param y1 y from first point
   * @param x2 x from second point
   * @param y2 y from second point
   * @return the distance
   */
  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    math.sqrt(math.pow((x1 - x2), 2) + math.pow((y1 - y2), 2))
  }

  /**
   * Finds the outliers by calculating the sum of distances from each point to every point in the same cluster
   * Outlier => point that the sum of distances deviates more than 3*std from the mean value of this cluster
   *
   * @param clusters contains the information for the clusters
   * @return the outliers
   */
  def distanceInCluster(clusters: RDD[Structs.ClusterIndex]):Seq[(Double,Double)] = {
    //Calculate for every element the distance from the elements in the same cluster
    val sums: RDD[Structs.ClusterDistances] = clusters.map(c => { //for every cluster
      val clusterDistances: Seq[Structs.Distance] = c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(element => { //for every coordinate
        val theSum = c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(inside => { //loouping through the rest
          euclideanDistance(element.getDouble(0), element.getDouble(1), inside.getDouble(0), inside.getDouble(1))
        }).sum
        Structs.Distance((element.getDouble(0), element.getDouble(1)), theSum)
      })
      Structs.ClusterDistances(c.cluster, clusterDistances, 0, 0)
    })

    //not that we have all the sums in 1 list, we can find the std and calculate the outliers
    val lists: RDD[Structs.ClusterDistances] = sums.map(row => {
      val list = row.distances.map(dRow => {
        dRow.distance
      }).toList
      val mean = list.sum / list.size
      val variance = list.map(a => math.pow(a - mean, 2)).sum / list.size
      val stdDev = math.sqrt(variance)
      Structs.ClusterDistances(row.cluster, row.distances, mean, stdDev) //Here we can keep all the metrics if necessary
    }).map(row => {
      val left: Seq[Structs.Distance] = row.distances.filter(dRow => { // keeps the points that deviates from mean value more than 3*std
        dRow.distance > row.mean + 4 * row.stdDev
      })
      Structs.ClusterDistances(row.cluster, left, 0, 0)
    }).filter(distance => { //clears out the clusters that dont have outliers
      distance.distances.size > 0
    })

    val outliers = new mutable.ArrayBuffer[(Double,Double)]()
    lists.collect().foreach(cd=>{ //keep only the coordinates from the outliers
      cd.distances.foreach(d=>{
        outliers.append(d.cordinates)
      })
    })
    outliers
  }

  /**
   * Finds the outliers by calculating the distances from each point to the center of the cluster.
   * Outlier => point that its distance from the center deviates over 3 times the std from the mean value
   *
   * @param clusters contains the information for the clusters
   * @return the outliers
   */
  def distanceFromCenters(clusters: RDD[Structs.ClusterIndex]): Seq[(Double, Double)] = {
    //get the distance from each point to the center of the custer
    val distances = clusters.map(c => {
      c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(cordinate => {
        val x = cordinate.getDouble(0)
        val y = cordinate.getDouble(1)
        Structs.Distance((x, y), euclideanDistance(x, y, c.center._1, c.center._2)) //calculates the distance from the center
      })
    })
    val lists: RDD[Seq[Structs.Distance]] = distances.map(c => {
      val list: List[Double] = c.map(point => {
        point.distance
      }).toList
      //find the mean and std and then repass all the elements to find which has distance > mean+3*std
      val mean = list.sum / list.size
      val variance = list.map(a => math.pow(a - mean, 2)).sum / list.size
      val stdDev = math.sqrt(variance)
      c.filter(point => { // the points that have distance form center of the cluster more than 3*std + mean
        point.distance > mean + 4 * stdDev
      })
    }).filter(list => {
      list.size > 0
    })
    val outliers = new mutable.ArrayBuffer[(Double, Double)]
    lists.collect().foreach(list => { //keep only the coordinates from the outliers
      list.foreach(point => {
        outliers.append(point.cordinates)
      })
    })

    outliers
  }

}
