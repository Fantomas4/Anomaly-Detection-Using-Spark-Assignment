import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import scala.collection.mutable
import java.util.Date


object AnomalyDetection {

  case class ClusterIndex(cluster:Int,sameCluster:Seq[(Double,Double)],center:(Double,Double))

  case class dataWithClusters(cordinates:(Double,Double),cluster:Int)

  case class Distance(cordinates:(Double,Double), distance:Double)

  case class ClusterDistances(cluster:Int,distances:Seq[Distance],mean:Double,stdDev:Double)

  def filterLine(line: String): Boolean = {

    // Split into words separated by a comma character
    val fields = line.split(",")


    if (fields.length == 2 && fields(0) == "") {
      false
    } else if (fields.length == 1) {
      false
    } else {
      true
    }
  }

  def parseLine(line: String): (Double, Double) = {

    val fields = line.split(",")

    (fields(0).toDouble, fields(1).toDouble)
  }

  def minmaxNormalization(pointTuples: RDD[(Double, Double)]): RDD[(Double, Double)] = {

    // Get the current spark session created in main()
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    val pointsDF = pointTuples.toDF("x", "y") //by doing it dataFrame you can assign column names

    import org.apache.spark.sql.functions._
    val maxMin = pointsDF.agg(max("x"), min("x"), max("y"), min("y")).head()
    val normalizedPointTuples = pointTuples.map(point => ((point._1 - maxMin.getDouble(1)) / (maxMin.getDouble(0) - maxMin.getDouble(1)), (point._2 - maxMin.getDouble(3)) / (maxMin.getDouble(2) - maxMin.getDouble(3))))
    normalizedPointTuples
  }

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
  def distanceInCluster(clusters: RDD[ClusterIndex]):Seq[(Double,Double)] = {
    //Calculate for every element the distance from the elements in the same cluster
    val sums: RDD[ClusterDistances] = clusters.map(c => { //for every cluster
      val clusterDistances: Seq[Distance] = c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(element => { //for every coordinate
        val theSum = c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(inside => { //loouping through the rest
          euclideanDistance(element.getDouble(0), element.getDouble(1), inside.getDouble(0), inside.getDouble(1))
        }).sum
        Distance((element.getDouble(0), element.getDouble(1)), theSum)
      })
      ClusterDistances(c.cluster, clusterDistances, 0, 0)
    })

    //not that we have all the sums in 1 list, we can find the std and calculate the outliers
    val lists: RDD[ClusterDistances] = sums.map(row => {
      val list = row.distances.map(dRow => {
        dRow.distance
      }).toList
      val mean = list.sum / list.size
      val variance = list.map(a => math.pow(a - mean, 2)).sum / list.size
      val stdDev = math.sqrt(variance)
      ClusterDistances(row.cluster, row.distances, mean, stdDev) //Here we can keep all the metrics if necessary
    }).map(row => {
      val left: Seq[Distance] = row.distances.filter(dRow => { // keeps the points that deviates from mean value more than 3*std
        dRow.distance > row.mean + 4 * row.stdDev
      })
      ClusterDistances(row.cluster, left, 0, 0)
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
  def distanceFromCenters(clusters: RDD[ClusterIndex]): Seq[(Double, Double)] = {
    //get the distance from each point to the center of the custer
    val distances = clusters.map(c => {
      c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(cordinate => {
        val x = cordinate.getDouble(0)
        val y = cordinate.getDouble(1)
        Distance((x, y), euclideanDistance(x, y, c.center._1, c.center._2)) //calculates the distance from the center
      })
    })
    val lists: RDD[Seq[Distance]] = distances.map(c => {
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





  def main(args: Array[String]) {
    val startingTime=System.nanoTime()
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("AnomalyDetection")
      .getOrCreate()

    val loadedLines = spark.sparkContext.textFile(args(0))

    val filteredLines = loadedLines.filter(filterLine) //kick values out that not contain both values

    val pointTuples = filteredLines.map(parseLine)

    val normPointTuples = minmaxNormalization(pointTuples)

    val pointVectors = normPointTuples.map(point => Vectors.dense(point._1, point._2)).persist(StorageLevel.MEMORY_AND_DISK)

    val numClusters = 100
    val numIterations = 50

    //run the model more than one time to reduce MSE, but it is too time consuming and Square Error is almost the same
    //so we will run it only one time
    var model:KMeansModel=null
    var cost:Double=Double.MaxValue
    for (_<- 0 to 0) {
      val newModel = KMeans.train(pointVectors, numClusters, numIterations)
      val newCost=newModel.computeCost(pointVectors)
      if (newCost<cost){
        cost=newCost
        model=newModel
      }
    }

    //predict the clusters for every value
    val clustered = model.predict(pointVectors).persist(StorageLevel.MEMORY_AND_DISK)

    import spark.implicits._

    // add the column of the predicted clusters together with the cordinates of the point
    val predicted: Dataset[dataWithClusters] = pointVectors.zip(clustered)
      .map(row => {
        dataWithClusters((row._1(0), row._1(1)), row._2.toInt)
      }).toDS().persist(StorageLevel.MEMORY_AND_DISK)

    //Clusters contains the cluster id, the sequence of all the points in this cluster and the cordinates of the center of the cluster
    val clusters: RDD[ClusterIndex] = predicted.select("cluster", "cordinates")
      .groupBy("cluster")
      .agg(collect_list("cordinates").alias("cordinates_list"))
      .rdd
      .map(row => {
        val clusterCenter=model.clusterCenters(row.getInt(0))
        ClusterIndex(row.getInt(0), row.getSeq[(Double, Double)](row.fieldIndex("cordinates_list")),(clusterCenter(0),clusterCenter(1)))
      }).persist(StorageLevel.MEMORY_AND_DISK)




    //method 1 calculate all the distances in a cluster,
//    val outliers1=distanceInCluster(clusters).foreach(println)

    //method 2 calculate only distance from center of clusters, O(n)
    val outliers=distanceFromCenters(clusters).foreach(println)


    clusters.unpersist()
    predicted.unpersist()
    val seconds=(System.nanoTime()-startingTime)/1000000000
    println("It took "+seconds+" seconds for the execution")
    // Stop the session
    spark.stop()

  }
}
