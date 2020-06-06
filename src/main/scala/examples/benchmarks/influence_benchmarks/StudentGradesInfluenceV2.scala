package examples.benchmarks.influence_benchmarks

import examples.benchmarks.AggregationFunctions
import examples.benchmarks.generators.StudentGradesDataGeneratorV2
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.RoaringBitmapProvenance
import provenance.rdd.{BottomNInfluenceTracker, FilterInfluenceTracker, PairProvenanceRDD, ProvenanceRDD, StreamingOutlierInfluenceTracker, TopNInfluenceTracker, UnionInfluenceTracker}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils


object StudentGradesInfluenceV2 {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var logFile = ""
    conf.setAppName("StudentGradesV2")
  
    if (args.length < 2) {
      conf.setMaster("local[*]")
      conf.setAppName("Student GradesV2").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesV2"
    } else {
      logFile = args(0)
      //local = args(1).toInt
    } //set up spark context
    
    val sc = new SparkContext(conf)
    val scdp = new SparkContextWithDP(sc)
    val lines = scdp.textFileProv(logFile)
    
    val courseGrades = lines.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      (courseId, grade)
    })
    //println("CS COURSE COUNT: " + courseGrades.filter(x => x._1.startsWith("CS")).count())
    //System.exit(1)
    //println("???: \n" + courseGrades.map(x => (x._1, 1)).reduceByKey(_+_).count())
    //println("CS COURSE Breakdowns: \n" + courseGrades.map(x => (x._1, grade)).reduceByKey(_+_)
                                                     //.collect()
                                              //.mkString("\n"))
    //System.exit(1)
    
    val courseGpas = courseGrades.mapValues(grade => {
      // https://pages.collegeboard.org/how-to-convert-gpa-4.0-scale
      if (grade >= 93) 4.0
      else if (grade >= 90) 3.7
      else if (grade >= 87) 3.3
      else if (grade >= 83) 3.0
      else if (grade >= 80) 2.7
      else if (grade >= 77) 2.3
      else if (grade >= 73) 2.0
      else if (grade >= 70) 1.7
      else if (grade >= 67) 1.3
      else if (grade >= 65) 1.0
      else 0.0
    })
    
    
    // Using the written out version for motivation example
    //val courseGpaAvgs = AggregationFunctions.averageByKey(courseGpas)
    val courseGpaAvgs =
    courseGpas.aggregateByKey((0.0, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})
    
    //    println("CS9 and CS11: " + courseGpaAvgs.take(5).union(
    //      courseGpaAvgs.filter(x => x._1 == "CS9" || x._1 == "CS11").collect()).mkString("\n"))
    //    System.exit(1)
    val deptGpas = courseGpaAvgs.map({case (courseId, gpa) =>
      // split once, at the first digit we find
      val dept = courseId.split("\\d", 2)(0).trim()
      (dept, gpa)
    })
  
    val deptGpaMeanVar = AggregationFunctions.averageAndVarianceByKeyWithInfluence(deptGpas)
  
    val out = deptGpaMeanVar
    
    Utils.runTraceAndPrintStats(out,
                                (row: (String, (Double, Double))) => row._1 == "CS",
                                lines,
                                (line: String) => {
                                  val arr = line.split(",")
                                  val courseId = arr(1)
                                  StudentGradesDataGeneratorV2.faultTargetCourses.contains(courseId)
                                })
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