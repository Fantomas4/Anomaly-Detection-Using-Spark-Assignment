import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.storage.StorageLevel

import math._
import org.apache.spark.sql.functions._

import scala.collection.mutable


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

  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    math.sqrt(math.pow((x1 - x2), 2) + math.pow((y1 - y2), 2))
  }

  def getSumDistance(predicted: Dataset[Row], row: Row): Double = {
    var sameCluster = null: Dataset[Row]
    try {
      sameCluster = predicted.select("x", "y").where("cluster==" + row.getInt(2))
    } catch {
      case e: Exception => println(row)
        return 0.0
    }
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val aSum: Double = sameCluster.map(crow => {
      euclideanDistance(row.getDouble(0), row.getDouble(1), crow.getDouble(0), crow.getDouble(1))
    }).agg(sum("value")).head().getDouble(0)
    aSum
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

    val loadedLines = spark.sparkContext.textFile("data201920.csv")

    println("Count of loaded entries: " + loadedLines.count.toString)

    val filteredLines = loadedLines.filter(filterLine) //kick values out that not contain both values

    val pointTuples = filteredLines.map(parseLine)

    val normPointTuples = minmaxNormalization(pointTuples)

    val pointVectors = normPointTuples.map(point => Vectors.dense(point._1, point._2)).persist(StorageLevel.MEMORY_AND_DISK)

    val numClusters = 5
    val numIterations = 50
    //TODO: run it 5 times and keep the model that minimize the sum of square error
    var model:KMeansModel=null
    var cost:Double=Double.MaxValue
    for (_<- 0 to 10) {
      val newModel = KMeans.train(pointVectors, numClusters, numIterations)
      val newCost=newModel.computeCost(pointVectors)
      println(newCost)
      if (newCost<cost){
        cost=newCost
        model=newModel
      }
    }

    val clustered = model.predict(pointVectors).persist(StorageLevel.MEMORY_AND_DISK)
    import spark.implicits._

    val predicted: Dataset[Structs.dataWithClusters] = pointVectors.zip(clustered)
      .map(row => {
        Structs.dataWithClusters((row._1(0), row._1(1)), row._2.toInt)
      }).toDS().persist(StorageLevel.MEMORY_AND_DISK)

    val clusters: RDD[Structs.ClusterIndex] = predicted.select("cluster", "cordinates")
      .groupBy("cluster")
      .agg(collect_list("cordinates").alias("cordinates_list"))
      .rdd
      .map(row => {
        Structs.ClusterIndex(row.getInt(0), row.getSeq[(Double, Double)](row.fieldIndex("cordinates_list")))
      })

    //Calculate for every element the distance from the elements in the same cluster
    val sums:RDD[Structs.ClusterDistances] = clusters.map(c => { //for every cluster
      val clusterDistances:Seq[Structs.SumDistance]=c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(element => { //for every coordinate
        val theSum =c.sameCluster.asInstanceOf[Seq[GenericRowWithSchema]].map(inside => { //loouping through the rest
          euclideanDistance(element.getDouble(0), element.getDouble(1), inside.getDouble(0), inside.getDouble(1))
        }).sum
        Structs.SumDistance((element.getDouble(0),element.getDouble(1)),theSum)
      })
      Structs.ClusterDistances(c.cluster,clusterDistances,0,0)
    })

    //not that we have all the sums in 1 list, we can find the std and calculate the outliers
    val lists:RDD[Structs.ClusterDistances]=sums.map(row=>{
      val list=row.distances.map(dRow=>{
        dRow.sum
      }).toList
      val mean = list.sum/ list.size
      val variance=list.map(a=>math.pow(a-mean,2)).sum/list.size
      val stdDev=math.sqrt(variance)
      Structs.ClusterDistances(row.cluster,row.distances,mean,stdDev) //Here we can keep all the metrics if necessary
    })

    val outliers=lists.map(row=>{
      val left:Seq[Structs.SumDistance]=row.distances.filter(dRow=>{
        dRow.sum>row.mean+4*row.stdDev || dRow.sum<row.mean-4*row.stdDev
      })
      Structs.ClusterDistances(row.cluster,left,0,0)
    })

    val test=outliers.collect() // this contains the calculated outliers for the 5 clusters



    predicted.unpersist()


    // Stop the session
    spark.stop()

  }
}
