object test {

  final case class Point(x: Option[Double], y: Option[Double])

  def main(args: Array[String]): Unit = {

    val testString = ",45"

    val fields = testString.split(",")

    val testObj = Point(Some(1.5), Some(3.1))

    println(testObj.x)
    println(testObj.y)

    println(Map(1 -> 7))

    print("mpla" + 3)

  }
}

