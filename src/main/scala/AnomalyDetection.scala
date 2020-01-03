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

  case class DataWithClusters(coordinates:(Double,Double), cluster:Int)

  case class Distance(coordinates:(Double,Double), distance:Double)

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

  def minmaxNormalization(pointTuples: RDD[(Double, Double)], maxMin: Row): RDD[(Double, Double)] = {

    val normalizedPointTuples = pointTuples.map(point => ((point._1 - maxMin.getDouble(1)) / (maxMin.getDouble(0) - maxMin.getDouble(1)), (point._2 - maxMin.getDouble(3)) / (maxMin.getDouble(2) - maxMin.getDouble(3))))
    normalizedPointTuples
  }

  def minmaxDenormalization(normalizedValues: Seq[(Double, Double)], maxMin: Row): Seq[(Double, Double)] = {

    val denormalizedValues = normalizedValues.map(point => ((maxMin.getDouble(0) - maxMin.getDouble(1)) * point._1 + maxMin.getDouble(1), (maxMin.getDouble(2) - maxMin.getDouble(3)) * point._2 + maxMin.getDouble(3)))

    denormalizedValues
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
   * Outlier => point that the sum of distances deviates more than 4*std from the mean value of this cluster
   *
   * @param clusters contains the information for the clusters
   * @return the outliers
   */
  def distanceInCluster(clusters: RDD[ClusterIndex]):Seq[(Double,Double)] = {
    //Calculate for every element the distance from the elements in the same cluster
    val sums: RDD[ClusterDistances] = clusters.map(c => { //for every cluster
      val clusterDistances: Seq[Distance] = c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(element => { //for every coordinate
        val theSum = c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(inside => { //looping through the rest
          euclideanDistance(element.getDouble(0), element.getDouble(1), inside.getDouble(0), inside.getDouble(1))
        }).sum //sum all the distances
        Distance((element.getDouble(0), element.getDouble(1)), theSum)
      })
      ClusterDistances(c.cluster, clusterDistances, 0, 0) //initialize with 0 mean and 0 stdDev
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
        outliers.append(d.coordinates)
      })
    })
    outliers
  }

  /**
   * Finds the outliers by calculating the distances from each point to the center of the cluster.
   * Outlier => point that its distance from the center deviates over 4 times the std from the mean value
   *
   * @param clusters contains the information for the clusters
   * @return the outliers
   */
  def distanceFromCenters(clusters: RDD[ClusterIndex]): Seq[(Double, Double)] = {
    //get the distance from each point to the center of the custer
    val distances = clusters.map(c => {
      c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(coordinate => {
        val x = coordinate.getDouble(0)
        val y = coordinate.getDouble(1)
        Distance((x, y), euclideanDistance(x, y, c.center._1, c.center._2)) //calculates the distance from the center
      })
    })
    distances.take(10).foreach(println) //printing

    val lists: RDD[Seq[Distance]] = distances.map(c => {
      val list: List[Double] = c.map(point => {
        point.distance
      }).toList
      //find the mean and std and then repass all the elements to find which has distance > mean+3*std
      val mean = list.sum / list.size
      val variance = list.map(a => math.pow(a - mean, 2)).sum / list.size
      val stdDev = math.sqrt(variance)
      c.filter(point => { // the points that have distance form center of the cluster more than 4*std + mean
        point.distance > mean + 4 * stdDev
      })
    }).filter(list => { //filters out all clusters without outliers
      list.size > 0
    })
    val outliers = new mutable.ArrayBuffer[(Double, Double)]
    lists.collect().foreach(seq => { //keep only the coordinates from the outliers
      seq.foreach(point => {
        outliers.append(point.coordinates)
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
      .master("local[*]")
      .getOrCreate()

    val loadedLines = spark.sparkContext.textFile(args(0))

    val filteredLines = loadedLines.filter(filterLine) //kick values out that not contain both values

    val pointTuples = filteredLines.map(parseLine)

    // Calculate the max and min value for the "x" and "y" coordinates
    // of all given points

    // First, convert pointTuples to a Data Frame in order to assign column names
    import spark.implicits._
    val pointsDF = pointTuples.toDF("x", "y")

    // Then, find the max and min values for x and y.
    // The result is saved inside a Row with the following format:
    // Row(max("x"), min("x"), max("y"), min("y"))
    import org.apache.spark.sql.functions._
    val maxMinOfCoordinates = pointsDF.agg(max("x"), min("x"), max("y"), min("y")).head()

    // Calculate the normalized points (x and y have values in [0,1])
    val normPointTuples = minmaxNormalization(pointTuples, maxMinOfCoordinates)

    // Prepare the points' data for processing by the k-means algorithm
    // by converting normPointTuples to an RDD[Vector] structure
    val pointVectors = normPointTuples.map(point => Vectors.dense(point._1, point._2)).persist(StorageLevel.MEMORY_AND_DISK)

    // Specify the number of clusters (k) that the k-means algorithm should look for
    // Specify the number of iterations the k-means algorithm should perform to train
    // the target model
    val numClusters = 100
    val numIterations = 50

    // Run the model more than one time to reduce MSE, but it is too time consuming and Square Error is almost the same
    // so we will run it only one time
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
    val clustered = model.predict(pointVectors)

    import spark.implicits._

    // add the column of the predicted clusters together with the coordinates of the point
    val predicted: Dataset[DataWithClusters] = pointVectors.zip(clustered)
      .map(row => {
        DataWithClusters((row._1(0), row._1(1)), row._2.toInt)
      }).toDS()
    predicted.show() //for printing

    //Clusters contains the cluster id, the sequence of all the points in this cluster and the coordinates of the center of the cluster
    val clusters: RDD[ClusterIndex] = predicted.select("cluster", "coordinates")
      .groupBy("cluster")
      .agg(collect_list("coordinates").alias("coordinates_list"))
      .rdd
      .map(row => {
        val clusterCenter=model.clusterCenters(row.getInt(0))
        ClusterIndex(row.getInt(0), row.getSeq[(Double, Double)](row.fieldIndex("coordinates_list")),(clusterCenter(0),clusterCenter(1)))
      })
    clusters.take(10).foreach(println) // print the clusters



    val normalizedOutliers = distanceFromCenters(clusters)

    minmaxDenormalization(normalizedOutliers, maxMinOfCoordinates).foreach(println) // printing results

    val seconds:Double=(System.nanoTime()-startingTime)/1000000000
    println("It took "+seconds+" seconds for the execution")
    // Stop the session
    spark.stop()

  }
}
