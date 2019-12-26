import org.apache.spark.sql._
import org.apache.log4j._


object AnomalyDetection {

  final case class Point(x: Option[Double], y: Option[Double])

  def parseLine(line: String) : Point = {

    println(line)

    // Split into words separated by a comma character
    val fields = line.split(",")

    if (fields.length == 2) {
      if (fields(0) == "") {
        Point(None, Some(fields(1).toDouble))
      } else {
        Point(Some(fields(0).toDouble), Some(fields(1).toDouble))
      }
    } else {
      // if fields.length == 1
      Point(Some(fields(0).toDouble), None)
    }
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

    val lines = spark.sparkContext.textFile("data201920.csv").map(parseLine)

    import spark.implicits._
    val pointsDS = lines.toDS()

//    pointsDS.collect().foreach(println)
    val pointElements = pointsDS.collect()

    for (elem <- pointElements) {

      val x = elem.x match {
        case None => ""//Or handle the lack of a value another way: throw an error, etc.
        case Some(i: Double) => i //return the number to set your value
      }

      val y = elem.y match {
        case None => ""//Or handle the lack of a value another way: throw an error, etc.
        case Some(i: Double) => i //return the number to set your value
      }

      println("x:" + x.toString + " y:" + y.toString)
    }


    // Stop the session
    spark.stop()

  }
}
