import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.ml.feature.MinMaxScaler


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

  final case class Point(x: Double, y: Double)

//  def parseLine(line: String) : Vector[Double] = {
//
//    val fields = line.split(",")
//
//    Vector(fields(0).toDouble, fields(1).toDouble)
//  }

  def parseLine(line: String) : Point = {

    val fields = line.split(",")

    Point(fields(0).toDouble, fields(1).toDouble)
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

//    val scaler = new MinMaxScaler()
//      .setInputCol("x")
//      .setOutputCol("x")
//      .setMax(1)
//      .setMin(0)
//
//    val normalizedLines = scaler.fit(filteredLines)

    val pointLines = filteredLines.map(parseLine)

    import spark.implicits._
    val pointsDS = pointLines.toDS()

    val xCol = pointsDS.select("x")
    xCol.collect.foreach(println)

    val yCol = pointsDS.select("y")
    yCol.collect.foreach(println)

    println("Count of filtered entries is: " + pointsDS.count.toString)

    // Stop the session
    spark.stop()

  }
}
