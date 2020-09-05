package examples.benchmarks.influence_benchmarks

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.{DummyProvenance, RoaringBitmapProvenance}
import symbolicprimitives.{SymInt, SymString, Utils}

import scala.collection.mutable

object Test {
  def main(args: Array[String]): Unit = {
    testStudentComboCounts()
    //testStudentCountComboTopBottom5s()
    //testStudentComboBottom5Counts()
    // testCourseCountByDept() redundant
  }

  def testSmall(): Unit = {
    val bmProv = RoaringBitmapProvenance.create(0)
    val test = ((SymString("MATH", bmProv), SymString("MATH149", bmProv)),
      (SymInt(78, bmProv), bmProv))
    val result = Utils.simplifySyms(test)
    println(result)
  }
  
  def testStudentComboCounts(): Unit = {
    val deptCount = studentDeptCourseGrades.map(_._1._1).countByValue()
  
    val courseDeptCount = studentDeptCourseGrades.map(_._1)
      .distinct()
      .countByKey()
    
    
    println("Dept count: " + deptCount)
    println("Courses per dept count: " + courseDeptCount)
    
  }
  
  lazy val courseGrades  = {
    studentDeptCourseGrades
      .map({ case ((dept, course), grade) => (course, grade) })
  }
  
  
  lazy val studentLines = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("TEST Student Grades Combo BigSift V3").set("spark.executor.memory", "2g")
    val logFile = "datasets/studentGradesCombo"
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("ERROR")
    
    val lines = sc.textFile(logFile)
    lines
  }
  
  def testStudentCountComboTopBottom5s(): Unit = {
  
    val deptCourseGrades = studentDeptCourseGrades
  
    val deptCourseAvgs = deptCourseGrades.aggregateByKey((0.0, 0))(
      { case ((sum, count), next) => (sum + next, count + 1) },
      { case ((sum1, count1), (sum2, count2)) => (sum1 + sum2, count1 + count2) }
                                                              ).mapValues({ case (sum, count) => sum / count })
  
    val lowestLimit = 5
    val highestLimit = 5
  
    val topBottomDeptValues =
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
    topBottomDeptValues.collect().foreach(println)
  }
  
  lazy val studentDeptCourseGrades = {
    val lines = studentLines
    
    lines.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      val dept = courseId.split("\\d", 2)(0).trim()
      ((dept, courseId), grade)
    })
  }
  
  def countByCourse(courses: Seq[String]): Map[String, Long] = {
      val filteredCourseGrades = courseGrades.filter(row => {
      val courseId = row._1
      courses.contains(courseId)
    })
  
    filteredCourseGrades.countByKey().toMap
  }
  def testStudentComboBottom5Counts(): Unit = {
    val result = countByCourse(
      Seq("Physics 83", "Physics 185", "Physics 187", "Physics 401", "Physics 402"))
    result.foreach(println)
    
    println("Total count: " + result.values.sum)
  }
  
  def testCourseCountByDept(): Unit = {
    val result = studentDeptCourseGrades
      .map({case ((dept, course), grade) => (dept, course)})
      .distinct()
      .countByKey()
    result.foreach(println)
  
    println("Total count: " + result.values.sum)
    
  }
}
