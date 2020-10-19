import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile.flatMap(line => line.split(',').drop(1))
                  .filter(rating => rating != "")

//    output.length.saveAsTextFile(args(1))

     val accum = sc.longAccumulator("ratings number count accumulator")
     sc.parallelize(Seq(output)).foreach(_ => accum.add(1))
     val count = accum.value
     textFile.flatMap(s => Seq(count)).saveAsTextFile(args(1))
  }
}
