package examples.benchmarks.baseline

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import symbolicprimitives.Utils

import scala.collection.mutable


object StudentGradesComboBaselineV3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var logFile = ""
    conf.setAppName("StudentGradesV2")
    
    if (args.length < 2) {
      conf.setMaster("local[*]")
      conf.setAppName("Student Grades Combo Baseline V3").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesComboSmall"
    } else {
      logFile = args(0)
      //local = args(1).toInt
    } //set up spark context
    
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val lines = sc.textFile(logFile)
    
    val deptCourseGrades = lines.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      val dept = courseId.split("\\d", 2)(0).trim()
      ((dept, courseId), grade)
    })
    
    val deptCourseAvgs = deptCourseGrades.aggregateByKey((0.0, 0))(
      { case ((sum, count), next) => (sum + next, count + 1) },
      { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
    ).mapValues({ case (sum, count) => sum / count })
    
    val lowestLimit = 3
    val highestLimit = 3
    
    val topBottomDeptAvgs =
    //taintedDeptCourseAvgs
      deptCourseAvgs
        // first get rid of the course key for our next agg
        .map({ case ((dept, course), avg) => (dept, avg) })
        // agg and retain the top/bottom 5 values in each dept key group
        .aggregateByKey(
          (new mutable.PriorityQueue[Double](),
            new mutable.PriorityQueue[Double]()(Ordering.Double.reverse)))(
          { case ((maxHeap, minHeap), avg) => {
            if (maxHeap.size < lowestLimit || avg < maxHeap.head) {
              maxHeap.enqueue(avg)
              while (maxHeap.size > lowestLimit) maxHeap.dequeue()
            }
            if (minHeap.size < highestLimit || avg > minHeap.head) {
              minHeap.enqueue(avg)
              while (minHeap.size > highestLimit) minHeap.dequeue()
            }
            (maxHeap, minHeap)
          }
          },
          { case ((maxHeapA, minHeapA), (maxHeapB, minHeapB)) => {
            maxHeapA ++= maxHeapB
            while (maxHeapA.size > lowestLimit) maxHeapA.dequeue()
            minHeapA ++= minHeapB
            while (minHeapA.size > lowestLimit) minHeapA.dequeue()
            (maxHeapA, minHeapA)
          }
          }
           )
        // using the top/bottom 5s, calculate the averages
        .mapValues({ case (maxHeap, minHeap) => {
          // chosen specifically for taint compatibility,
          // otherwise we need to implement implicit methods for traversableonce and taints
          // similar to how sum is implemented.
          val bottomAvg = maxHeap.reduce(_ + _)
          val bottomSize = maxHeap.size
          val topAvg = minHeap.reduce(_ + _)
          val topSize = minHeap.size
          (bottomAvg / bottomSize, topAvg / topSize)
        }
        })
    
    val out = topBottomDeptAvgs
    //Utils.runBaselineTest(deptCourseAvgs) // seems OK, contains 4 values per tainted object
    Utils.runBaseline(out)
  }
}