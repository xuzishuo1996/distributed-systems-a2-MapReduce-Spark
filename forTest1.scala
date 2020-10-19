import scala.collection.mutable.ListBuffer

object forTest1 {
  def main(args: Array[String]) {
    //    val dim: List[List[(Int, Int)]] =
    //      List(
    //        List((1,2), (3,4), (5,6)),
    //        List((0,1)),
    //        List((0,0), (1,0), (0,1))
    //      )
    //    val flattened = dim.flatten
    //    flattened.reduceByKey
    //    println(flattened)
    val dim = List("5", "6", "", "8", "6")
    val res = dim.zipWithIndex.map { case (score, user) => {
      var times: Int = 0
      if (score.equals("")) {
        times = 0
      } else {
        times = 1
      }
      (user, times)
    }
    }
    val a = 1
  }
}
