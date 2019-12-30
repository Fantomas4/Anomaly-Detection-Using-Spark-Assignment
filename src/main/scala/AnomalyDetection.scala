import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.apache.spark.sql.functions._



object AnomalyDetection {

  def filterLine(line: String): Boolean = {

    // Split into words separated by a comma character
    val fields = line.split(",")

    // if form "number1, number2" is encountered, then fields.length is 2
    // if form "number1" is encountered, then fields.length is 1
    // if form ",number2" is encountered, then fields.length is 2 --> fields(0) = "", fields(1) = "number2"

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
    println("xColMax: " + maxMin.getDouble(0))
    println("xColMin: " + maxMin.getDouble(1))
    println("yColMax: " + maxMin.getDouble(2))
    println("yColMin: " + maxMin.getDouble(3))

    //changed ths sorting to using max and min aggregation, this comes from meta data so it is O(1)
    //    val xMax = pointsDS.select("_1").orderBy(desc("_1")).first().getDouble(0)
    //    val xMin = pointsDS.select("_1").orderBy(asc("_1")).first().getDouble(0)
    //    val yMax = pointsDS.select("_2").orderBy(desc("_2")).first().getDouble(0)
    //    val yMin = pointsDS.select("_2").orderBy(asc("_2")).first().getDouble(0)

    val normalizedPointTuples = pointTuples.map(point => ((point._1 - maxMin.getDouble(1)) / (maxMin.getDouble(0) - maxMin.getDouble(1)), (point._2 - maxMin.getDouble(3)) / (maxMin.getDouble(2) - maxMin.getDouble(3))))
    normalizedPointTuples
  }






  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("AnomalyDetection")
      .master("local[*]")
      .getOrCreate()

    val loadedLines = spark.sparkContext.textFile(args(0))

    println("Count of loaded entries: " + loadedLines.count.toString)

    val filteredLines = loadedLines.filter(filterLine) //kick values out that not contain both values

    val pointTuples = filteredLines.map(parseLine)

    val normPointTuples = minmaxNormalization(pointTuples)

    val pointVectors = normPointTuples.map(point => Vectors.dense(point._1, point._2)).persist(StorageLevel.MEMORY_AND_DISK)

    val numClusters = 100
    val numIterations = 50

    //run the model more than one time to
    var model:KMeansModel=null
    var cost:Double=Double.MaxValue
    for (_<- 0 to 0) {
      val newModel = KMeans.train(pointVectors, numClusters, numIterations)
      val newCost=newModel.computeCost(pointVectors)
      println(newCost)
      if (newCost<cost){
        cost=newCost
        model=newModel
      }
    }

    //predict the v
    val clustered = model.predict(pointVectors).persist(StorageLevel.MEMORY_AND_DISK)

    import spark.implicits._

    // add the column of the predicted clusters together with the cordinates of the point
    val predicted: Dataset[Structs.dataWithClusters] = pointVectors.zip(clustered)
      .map(row => {
        Structs.dataWithClusters((row._1(0), row._1(1)), row._2.toInt)
      }).toDS().persist(StorageLevel.MEMORY_AND_DISK)

    //Clusters contains the cluster id, the sequence of all the points in this cluster and the cordinates of the center of the cluster
    val clusters: RDD[Structs.ClusterIndex] = predicted.select("cluster", "cordinates")
      .groupBy("cluster")
      .agg(collect_list("cordinates").alias("cordinates_list"))
      .rdd
      .map(row => {
        val clusterCenter=model.clusterCenters(row.getInt(0))
        Structs.ClusterIndex(row.getInt(0), row.getSeq[(Double, Double)](row.fieldIndex("cordinates_list")),(clusterCenter(0),clusterCenter(1)))
      }).persist(StorageLevel.MEMORY_AND_DISK)




    //method 1 calculate all the distances in a cluster,
    val outliers1=Methods.distanceInCluster(clusters).foreach(println)

    //method 2 calculate only distance from center of clusters, O(n)
    val outliers2=Methods.distanceFromCenters(clusters).foreach(println)


    clusters.unpersist()
    predicted.unpersist()

    // Stop the session
    spark.stop()

  }
}
