import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => line.split(",", -1).drop(1))
      .filter(rating => rating != "")
      .count()

    sc.parallelize(Seq(output)).saveAsTextFile(args(1))
  }
}

//object Task2 {
//  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("Task 2").setMaster("local")
//    val sc = new SparkContext(conf)
//
//    // for test locally
//    val inPath = "sample_input/smalldata.txt"
//    val outPath = "my_output/scala2.out"
//    val textFile = sc.textFile(inPath)
//
//    // modify this code
//    val output = textFile.flatMap(line => line.split(",", -1).drop(1))
//      .filter(rating => rating != "")
//      .count()
//
//    sc.parallelize(Seq(output)).saveAsTextFile(outPath)
//
//
////    // wrong
////    output.length.saveAsTextFile(outPath)
//
////    // wrong
////    val accum = sc.longAccumulator("ratings number count accumulator")
////    sc.parallelize(Seq(output)).foreach(_ => accum.add(1))
////    val count = accum.value
////    println(count)
////    textFile.flatMap(s => Seq(count)).saveAsTextFile(outPath)
//  }
//}
