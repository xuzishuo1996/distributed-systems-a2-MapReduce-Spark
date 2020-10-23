import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task4 {
//  def calcSimilarity(array1: Array[String], array2:  Array[String]): String = {
//    val ratings1 = array1.drop(1).map(rating => {
//      if (rating.nonEmpty) {
//        rating.toInt
//      } else {
//        0
//      }
//    })
//
//    val ratings2 = array2.drop(1).map(rating => {
//      if (rating.nonEmpty) {
//        rating.toInt
//      } else {
//        0
//      }
//    })
//
//    var similarity = 0
//    for (i <- ratings1.indices) {
//      if (ratings1(i) != 0 && ratings1(i) == ratings2(i)) {
//        similarity += 1
//      }
//    }
//
//    array1(0) + ',' + array2(0) + ',' + similarity
//  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val lines = textFile.map(line => line.split(",", -1))
    val broadcastLines = sc.broadcast(lines.collect())
    val linesRDD = sc.parallelize(broadcastLines.value)
    linesRDD.cache()
    val output = linesRDD.cartesian(linesRDD)
      .filter(pair => pair._1(0).compareTo(pair._2(0)) < 0)
      .map(pair => {

        val ratings1 = pair._1.drop(1).map(rating => {
          if (rating.nonEmpty) {
            rating.toInt
          } else {
            0
          }
        })

        val ratings2 = pair._2.drop(1).map(rating => {
          if (rating.nonEmpty) {
            rating.toInt
          } else {
            0
          }
        })

        var similarity = 0
        for (i <- ratings1.indices) {
          if (ratings1(i) != 0 && ratings1(i) == ratings2(i)) {
            similarity += 1
          }
        }

        pair._1(0) + "," + pair._2(0) + "," + similarity
      })

    output.saveAsTextFile(args(1))
  }


//  /* for local test */
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("Task 4").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    val textFile = sc.textFile("sample_input/smalldata.txt")
//
//    // modify this code
//    val lines = textFile.map(line => line.split(",", -1))
//    val output = lines.cartesian(lines)
//      .filter(pair => pair._1(0).compareTo(pair._2(0)) < 0)
//      .map(pair => calcSimilarity(pair._1, pair._2))
//
//    output.saveAsTextFile("my_output/scala4.out")
//  }
}
