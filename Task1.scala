import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(x => {
      var ratings = x.split(",")
      var movie = ratings(0)

      var max = 0
      var maxUserList = new ListBuffer[String]

      // idx starts from 0, 0 is movie name
      for (i <- 1 until ratings.length) {
        if (ratings(i) != "") {
          val rating = ratings(i).toInt
          if (rating == max) {
            maxUserList += i
          } else if (rating > max) {
            max = rating
            maxUserList.clear()
            maxUserList += i
            // maxUserList.append(i)
          }
        }
      }

      movie + "," + maxUserList.mkString(",")
    })
    
    output.saveAsTextFile(args(1))
  }
}
