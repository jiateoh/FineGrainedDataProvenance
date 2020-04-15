package examples

import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import sparkwrapper.SparkContextWithDP

// jteoh: Mix of gulzar's work, ml.KMeansExample, and gist @ https://gist.github.com/umbertogriffo/b599aa9b9a156bb1e8775c8cdbfb688a
object KMeansNetworkWithProvenance extends App {
  
  def parseVector(line: String): Array[Double] =
    line.split(" ").map(_.toDouble)
  
  def squaredDistance(p: Array[Double],
                      center: Array[Double]): Double= {
    p.zip(center).map(s => s._1 - s._2).map(s => s * s).sum
  }
  
  def closestCenter(p: Array[Double],
                    centers: Array[Array[Double]]): Int = {
    centers.indices.minBy(i => squaredDistance(p, centers(i)))
    /**
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    
    for (i <- centers.indices) {
      val tempDist = squaredDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
   **/
  }
  
  def updateIteration(oldCenters: Array[Array[Double]],
                      newCenters: Array[Array[Double]]) = {
    
  }
  
  val conf = new SparkConf().setMaster("local[*]").set("spark.eventLog.dir", "/tmp/spark-events")
                            .set("spark.eventLog.enabled", "true")
  val spark = SparkSession.builder
                          .config(conf)
                .appName("SparkKMeans")
                .master("local[*]")
                .getOrCreate()
  val _sc = spark.sparkContext
  _sc.setLogLevel("WARN")
  val sc = new SparkContextWithDP(_sc)
  
  val lines = sc.textFile(args.headOption
                          .getOrElse("/Users/jteoh/Code/distributions/spark-2.2.0-bin-hadoop2.7" +
                                       "/data/mllib/kmeans_data.txt"))
    //"/Users/malig/workspace/git/ADSpark/src/main/resources/delivery_data.txt"
  //var data = lines.map(s => parseVector(s)).cache()

  // local testing only, data format is different...
  var data = sc.textFileProv("/Users/jteoh/Code/FineGrainedDataProvenance/kmeans_sample_data")
    .map(line => line.split(','))
    .map(pair => Array(pair(3).toDouble, pair(4).toDouble))
    .filter(point => !point.contains(0.0))
  
  data.persist()
  
  val K = args.lift(1).map(_.toInt).getOrElse(2) // number of clusters
  val convergeDist = args.lift(2).map(_.toDouble).getOrElse(0.1) // based on gist
  val maxIter = args.lift(3).map(_.toInt).getOrElse(20) // based on ml.KMeans defaults
  val seed = args.lift(4).map(_.toInt).getOrElse(42)
  
  //val kPoints: Array[Array[Double]] = data.takeSample(withReplacement = false, K, seed)
  data.takeSampleWithProvenance(withReplacement = false, K, seed)
  val kPoints: Array[Array[Double]] = data.takeSampleWithProvenance(withReplacement = false, K, seed)
  var tempDist = Double.PositiveInfinity // used to measure convergence rate
  var iteration = 1
  

  // Key differences between simple and library implementation:
  // (1) Initialization options - uses simple random as opposed to optimized version. Should be fine for our use case.
  // (2) broadcasting the centers rather than serializing tasks (should be negligible for small K)
  // (3) mapPartitions to optimize the cluster-sum+count outputs, as opposed to relying solely on reduceByKey

  // Detailed description of how it works in distributed fashion:
  // (0) initialize center points (typically done with KMeansParallel approach, but we use random for simplicity)
  // each iteration (until max iterations or convergence as defined in 7.1)
  // (1) broadcast the center points
  // (2) create a costAccumulator (double) - (this isn't strictly required though, just to measure accuracy of result)
  // (3) map data per partition:
  // (3.0): initialize (a) sum-vectors for each center, and (b) count for each center.
  // (3.1) for each point, find its closest center and the cost (distance)
  // (3.2) add cost to the cost costAccumulator
  // (3.3) for the closest center: add to sum (using axpy) and increment count (from 3.0)
  //    y += a*x, with a = 1, x = point-vector, y = sum (in other words, sum the vectors)
  // (3.4) produce (center_index, (sum_index, count_index)) for each center with non-zero count
  // (4) reduceByKey: for each center: add the sum-vectors and the counts together.
  // (5) collectAsMap: these will be used to later calculate the new centers
  // (6) delete broadcast variable from (1)
  // (7) update the collected map to compute centers (sum-vector / scalar count)
  // (7.1) checking if distance between old and new center is within epsilon^2 (convergence)
  // (8) Update cost (from accumulator) and iteration counter
  // (8.1) cost is actually never really used, except for one print at the end of all execution
  while (tempDist > convergeDist && iteration <= maxIter) {
    // find the closest center for each point. also print out (point, 1) similar to wordcount...
    val closest = data.map(p => (closestCenter(p, kPoints), (p, 1)))
    
    // for each center, sum the points and the counts so we can average them later.
    val pointStats = closest.reduceByKey {
      case ((p1, c1), (p2, c2)) =>
        (p1.zip(p2).map(s => s._1 + s._2), c1 + c2)
    }
    
    // compute the average point, done by dividing the summed point (vector) by count.
    val newPoints = pointStats
                    .map { pair =>
                      (pair._1, pair._2._1.map(s => s / pair._2._2))
                    }
                    .collectAsMap()
    
    // Calculate the rate of convergence (how much our centers moved since previous iteration)
    tempDist = 0.0
    for (i <- 0 until K) {
      tempDist = tempDist + squaredDistance(kPoints(i), newPoints(i))
    }
  
    updateIteration(kPoints, newPoints)
    // Update our centers with the newly calculated ones.
    for (newP <- newPoints) {
      kPoints(newP._1) = newP._2
    }
    
    println(s"Finished iteration $iteration (delta = $tempDist)")
    
    iteration += 1
  }
  
  println("Final centers:")
  println("====")
  kPoints.foreach(center => { println(center.mkString("[", ",", "]")); println("====") })
  spark.stop()

}