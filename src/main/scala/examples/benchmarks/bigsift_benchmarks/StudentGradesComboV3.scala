package examples.benchmarks.bigsift_benchmarks

import examples.benchmarks.bigsift.BigSift
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.ui.BSListenerBusImpl
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object StudentGradesComboV3 {
  var bus: BSListenerBusImpl = null
  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[*]")
      sparkConf.setAppName("Student Grades Combo BigSift V3").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesCombo"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val bs = new BigSift(ctx, logFile)
    bs.runWithBigSift[(String, (Double, Double))](app, Some(failure))
    bs.getDebuggingStatistics()
  }

  def app(input: RDD[String], lineage: Lineage[String]): RDD[(String, (Double, Double))] = {
    if (lineage != null) {
      fun(lineage)
    } else {
      fun(input)
    }
  }

  def fun(input: Lineage[String]): RDD[(String, (Double, Double))] = {
    val deptCourseGrades = input.map(line => {
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
    out
  }
  def fun(input: RDD[String]): RDD[(String, (Double, Double))] = {
    val deptCourseGrades = input.map(line => {
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
    
    out
  }

  def failure(record: (String, (Double, Double))): Boolean = {
    record._2._1 < 80 || record._2._2 > 85
  }
}

