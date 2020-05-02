package examples.benchmarks.provenance_benchmarks

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
// import utils.SparkRDDGenerator

/**
  * Created by malig on 3/27/18.
  * Copied by jteoh and adapted on 4/27/20 from
  * https://github.com/maligulzar/BigTest/blob/JPF-integrated/BenchmarksFault/src/gradeanalysis/StudentGrades.scala
  */
object StudentGradesProvenance { // extends SparkRDDGenerator
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var logFile = ""
    conf.setAppName("StudentGrades")
//    val data1 = Array(":0\n:0",	":0\n:41",	":0\n ,:0",	":0\n ,:41",	":41\n:0"	,":41\n:41"	,":41\n ,:0",	":41\n ,:41",	" ,:0\n:0"	,
//                      " ,:0\n:41"	," ,:0\n ,:0",	" ,:0\n ,:41"	," ,:41\n ,:0",	" ,:41\n:41",	" ,:41\n ,:0",	" ,:41\n ,:41",	"",	" ,")
  
    if (args.length < 2) {
      conf.setMaster("local[*]")
      conf.setAppName("Student Grades").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGrades"
    } else {
      logFile = args(0)
      //local = args(1).toInt
    } //set up spark context
    
    val startTime = System.currentTimeMillis();
    val sc = new SparkContext(conf)
    val scdp = new SparkContextWithDP(sc)
    val lines = scdp.textFileProv(logFile)
    
    val courseGradeStrs = lines.flatMap( line =>line.split(","))
    val courseGrades = courseGradeStrs.map{  s =>
                   val a = s.split(":")
                   (a(0) , a(1).toInt)
                 }
    val passFails = courseGrades.map { a =>
             if (a._2 > 40)
               (a._1 + " Pass", 1)
             else
               (a._1 + " Fail", 1)
           }
           .reduceByKey(_ + _)
           .filter(v => v._2 > 1)
    val out = passFails.collect()
                 //.map(m => m._1 +","+ m._2)
    out.foreach(println)
    
    println("Time: " + (System.currentTimeMillis() - startTime))
  }
  
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