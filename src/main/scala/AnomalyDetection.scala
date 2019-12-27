import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object AnomalyDetection {

  def filterLine(line: String) : Boolean = {

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

  def parseLine(line: String) : (Double, Double) = {

    val fields = line.split(",")

    (fields(0).toDouble, fields(1).toDouble)
  }

  def minmaxNormalization(pointTuples: RDD[(Double, Double)]) : RDD[(Double, Double)] = {

    // Get the current spark session created in main()
    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    val pointsDS = pointTuples.toDS()

    import org.apache.spark.sql.functions._
    val xMax = pointsDS.select("_1").orderBy(desc("_1")).first().getDouble(0)
    val xMin = pointsDS.select("_1").orderBy(asc("_1")).first().getDouble(0)
    println("xColMax: " + xMax)
    println("xColMin: " + xMin)

    val yMax = pointsDS.select("_2").orderBy(desc("_2")).first().getDouble(0)
    val yMin = pointsDS.select("_2").orderBy(asc("_2")).first().getDouble(0)
    println("yColMax: " + yMax)
    println("yColMin: " + yMin)

    val normalizedPointTuples = pointTuples.map(point => ((point._1 - xMin) / (xMax - xMin), (point._2 - yMin) / (yMax - yMin)))
    normalizedPointTuples.foreach(println)

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

    val loadedLines = spark.sparkContext.textFile("data201920.csv")

    println("Count of loaded entries: " + loadedLines.count.toString)

    val filteredLines = loadedLines.filter(filterLine)

    val pointTuples = filteredLines.map(parseLine)

    val normPointTuples = minmaxNormalization(pointTuples)

    val pointVectors = normPointTuples.map(point => Vectors.dense(point._1, point._2))

    // Cluster the data into two classes using KMeans
    val numClusters = 4
    val numIterations = 50
    val clusters = KMeans.train(pointVectors, numClusters, numIterations)

    // Stop the session
    spark.stop()

  }
}
