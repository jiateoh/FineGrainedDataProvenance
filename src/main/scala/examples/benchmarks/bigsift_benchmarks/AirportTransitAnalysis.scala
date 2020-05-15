package examples.benchmarks.bigsift_benchmarks

import examples.benchmarks.bigsift.BigSift
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.ui.BSListenerBusImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ali on 7/20/17.
  */
object AirportTransitAnalysis {
  var bus: BSListenerBusImpl = null

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Airport Transit Time Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/airportdata/*"
    } else {
      logFile = args(0)
    }
    val ctx = new SparkContext(sparkConf)
    val bsift = new BigSift(ctx, logFile)
    bsift.runWithBigSift[(((String, String), Int))](app,Some(failure))
    bsift.getDebuggingStatistics()
  }

  def app(input: RDD[String] , lineage: Lineage[String]): RDD[(((String, String), Int))] = {
    if(lineage!= null){
      program(lineage)
    }else{
     program(input)
    }
  }

  def program (input: RDD[String] ): RDD[(((String, String), Int))] ={
    val map = input.map { s =>
      val tokens = s.split(",")
      val dept_hr = tokens(3).split(":")(0)
      val diff = getDiff(tokens(2), tokens(3))
      val airport = tokens(4)
      ((airport, dept_hr), diff)
    }
    val fil = map.filter { v =>
      v._2 < 45
    }
    val out = fil.reduceByKey(_ + _)
    out
  }
  def getDiff(arr: String, dep: String): Int = {
    val arr_min = arr.split(":")(0).toInt * 60 + arr.split(":")(1).toInt
    val dep_min = dep.split(":")(0).toInt * 60 + dep.split(":")(1).toInt
     if(dep_min - arr_min < 0){
      return 24*60 + dep_min - arr_min
     }
    return dep_min - arr_min
  }


  def program (input: Lineage[String] ): RDD[(((String, String), Int))] ={
    val map = input.map { s =>
      val tokens = s.split(",")
      val dept_hr = tokens(3).split(":")(0)
      val diff = getDiff(tokens(2), tokens(3))
      val airport = tokens(4)
      ((airport, dept_hr), diff)
    }
    val fil = map.filter { v =>
      v._2 < 45
    }
    val out = fil.reduceByKey(_ + _)
    out
  }

  def failure(record: (Any, Int)): Boolean = {
    record.asInstanceOf[(Any, Int)]._2 < 0
  }



}