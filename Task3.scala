import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.map(line => line.split(',').drop(1).zipWithIndex.map{case (rating, user) => {
      var times = 1;
      if (rating.equals("")) {
        times = 0;
      }
      (user + 1, times)
    }})
      .flatMap(x => x)
      .reduceByKey(_ + _)
      .map(x => x._1 + "," + x._2)

    output.saveAsTextFile(args(1))
  }
}


/* for local test */
//object Task3 {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("Task 3").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    // for test locally
//    val inPath = "sample_input/smalldata.txt"
//    val outPath = "my_output/scala3.out"
//    val textFile = sc.textFile(inPath)
////    val textFile = sc.textFile(args(0))
//
//    // modify this code
////    val output = textFile.map(line => {
////      val ratings = line.split(',')
////      val ratedUserList = new ListBuffer[(Int, Int)]
////
////      // idx starts from 0, 0 is movie name
////      for (i <- 1 until ratings.length) {
////        if (ratings(i) != "") {
////          ratedUserList.append((i, 1))
////        } else {
////          ratedUserList.append((i, 0))
////        }
////      }
////
////      ratedUserList.toList
////    })
//
//    val output = textFile.map(line => line.split(',').drop(1).zipWithIndex.map{case (rating, user) => {
//      var times = 1;
//      if (rating.equals("")) {
//        times = 0;
//      }
//      (user + 1, times)
//    }})
//      .flatMap(x => x)
//      .reduceByKey(_ + _)
//      .map(x => x._1 + "," + x._2)
//
//    output.saveAsTextFile(outPath)
////    output.saveAsTextFile(args(1))
//  }
//}
