package examples.benchmarks.influence_benchmarks

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils

import scala.collection.mutable

/** Despite the name, this also includes tainting */
object StudentGradesComboInfluence {
  class DepartmentPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key.asInstanceOf[(String, String)]._1.hashCode % numPartitions
  }
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var logFile = ""
    conf.setAppName("StudentGradesV2")
  
    if (args.length < 2) {
      conf.setMaster("local[*]")
      conf.setAppName("Student GradesV2").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesCombo"
    } else {
      logFile = args(0)
      //local = args(1).toInt
    } //set up spark context
    
    val sc = new SparkContext(conf)
    val scdp = new SparkContextWithDP(sc)
    val lines = scdp.textFile(logFile)
    
    val deptCourseGrades = lines.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      val dept = courseId.split("\\d", 2)(0).trim()
      ((dept, courseId), grade)
    })
    
    val deptCourseAvgs = deptCourseGrades.aggregateByKey((0.0, 0), new DepartmentPartitioner(5))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}//,
//                                                          enableUDFAwareProv = Some(false),
//                                                          influenceTrackerCtr = None
      // TODO influence function here
    ).mapValues({case (sum, count) => sum.toDouble/count})
    
    
    val lowestLimit = 5
    val highestLimit = 5
    val topBottomDeptAvgs: RDD[(String, Double, Double)] = deptCourseAvgs.mapPartitions(partition => {
      // value is maxHeap followed by minHeap, to retain lowest5 and highest5 (by popping the
      // highest/lowest each time)
      val deptMap = new mutable.HashMap[String,
        (mutable.PriorityQueue[Double],
          mutable.PriorityQueue[Double])
      ]()
      // course id unused
      partition.foreach({ case ((dept, _), avg) =>
        val (maxHeap, minHeap) = deptMap.getOrElseUpdate(dept,
                                                         (new mutable.PriorityQueue[Double](),
                                                           new mutable.PriorityQueue[Double]()(Ordering.Double.reverse)))
        
  
  
        if (maxHeap.size < lowestLimit || avg < maxHeap.head) {
          maxHeap.enqueue(avg)
          while (maxHeap.size > lowestLimit) maxHeap.dequeue()
        }
        if (minHeap.size < lowestLimit || avg > minHeap.head) {
          minHeap.enqueue(avg)
          while (minHeap.size > highestLimit) minHeap.dequeue()
        }
      })
      deptMap.iterator.map({ case (dept, (maxHeap, minHeap)) =>
        (dept, maxHeap.sum / maxHeap.size, minHeap.sum / minHeap.size)
      })
    })
    
    val out = topBottomDeptAvgs
    Utils.runBaseline(out)
    
//    val elapsed = Utils.measureTimeMillis({
//      val outCollect = out.collectWithProvenance()
//      println("Department, (Mean, Variance)")
//      outCollect.foreach(println)
//
//      // Debugging
//      val csRecord = outCollect.filter(_._1._1 == "CS").head // get the CS row
//      val csProvenance = csRecord._2
//      val trace = Utils.retrieveProvenance(csProvenance)
//      println("----- TRACE RESULTS ------")
//      println("Count = " + trace.count())
//      //trace.take(100).foreach(println)
//    })
//    println(s"Elapsed time: $elapsed ms")
  }
//
//  override def execute(input1: RDD[String], input2: RDD[String]): RDD[String] = {
//    input1.flatMap(l => l.split("\n")).flatMap{ line =>
//      val arr = line.split(",")
//      arr
//    }
//          .map{  s =>
//            val a = s.split(":")
//            (a(0) , a(1).toInt)
//          }
//          .map { a =>
//            if (a._2 > 40)
//              (a._1 + " Pass", 1)
//            else
//              (a._1 + " Fail", 1)
//          }
//          .reduceByKey(_ + _)
//          .filter(v => v._2 > 1).map(m => m._1 +","+ m._2)
//  }

}
//
//
///**
//  *
//  *
//val text = sc.textFile("hdfs://scai01.cs.ucla.edu:9000/clash/datasets/bigsift/studentGrades/*").sample(false, 0.001)
//text.cache
//  text.count
//text.flatMap{ line =>
//val arr = line.split(",")
//arr
//}.map{  s =>
//  val a = s.split(":")
//        (a(0) , a(1).toInt)
//      }.map { a =>
//        if (a._2 > 40)
//          (a._1 + " Pass", 1)
//        else
//          (a._1 + " Fail", 1)
//      }.reduceByKey(_ + _).filter(v => v._2 > 1).count
//  */
//  */
//
///***
//Big Test Conf
//filter1 > "",1
//map3> "",1
//map4 > "CS:123"
//reduceByKey2 > {1,2,3,4}
//flatMap5 > "a,a"
//DAG >filter1-reduceByKey2:reduceByKey2-map3:map3-map4:map4-flatMap5
//  */