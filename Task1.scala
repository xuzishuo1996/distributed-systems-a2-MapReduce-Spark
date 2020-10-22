import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ListBuffer}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
//    for (i <- args.indices) {
//      println(args(i))
//    }

    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => {
      val ratings = line.split(",", -1)
      val movie = ratings(0)

      var max = 0
      var maxUserList = new ListBuffer[Int]

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


/* for local test */
//object Task1 {
//  def main(args: Array[String]) {
//    //    val conf = new SparkConf().setAppName("Task 1")
//
//    // for test locally
//    val conf = new SparkConf().setAppName("Task 1").setMaster("local")
//
//    val sc = new SparkContext(conf)
//
//    // for test locally
//    val inPath = "sample_input/smalldata.txt"
//    val outPath = "my_output/scala1.out"
//    val textFile = sc.textFile(inPath)
//
//    //    val textFile = sc.textFile(args(0))
//
//    // modify this code
//    val output = textFile.map(line => {
//      val ratings = line.split(",", -1)
//      val movie = ratings(0)
//
//      var max = 0
//      var maxUserList = new ListBuffer[Int]
//
//      // idx starts from 0, 0 is movie name
//      for (i <- 1 until ratings.length) {
//        if (ratings(i) != "") {
//          val rating = ratings(i).toInt
//          if (rating == max) {
//            maxUserList += i
//          } else if (rating > max) {
//            max = rating
//            maxUserList.clear()
//            maxUserList += i
//            // maxUserList.append(i)
//          }
//        }
//      }
//
//      movie + "," + maxUserList.mkString(",")
//    })
//
//    //    output.saveAsTextFile(args(1))
//
//    // for test locally
//    output.saveAsTextFile(outPath)
//  }
//}
