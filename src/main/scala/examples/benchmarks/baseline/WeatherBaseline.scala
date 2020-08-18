package examples.benchmarks.baseline

import org.apache.spark.{SparkConf, SparkContext}
import symbolicprimitives.Utils

/**
  * Created by ali on 2/25/17.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/weather/WeatherAnalysis.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object WeatherBaseline {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Weather Analysis Baseline").set("spark.executor.memory", "2g")
      logFile = "datasets/weatherdata"
    } else {
      logFile = args(0)
      local = args(1).toInt
    }
    
    
    //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val input = ctx.textFile(logFile)

    val split = input.flatMap { s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf('/'))
      // gets month / date
      val monthdate = date.substring(0, date.lastIndexOf('/'))
      List[(String, Float)](
        (monthdate, snow),
        (year, snow)
      ).iterator
    }
    
    //split.rdd.asInstanceOf[RDD[((SymString, SymFloat), Provenance)]].filter(_._2.count > 2)
    //     .take(100).foreach(println)
    //split.takeWithProvenance(10).foreach(println)
    //println("TADA!")
    //System.exit(1)
    
    val deltaSnow = split.aggregateByKey((Float.MaxValue, Float.MinValue))(
      { case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next)) },
      { case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB)) })
                         .mapValues({ case (min, max) => max - min })


    Utils.runBaseline(deltaSnow)
    
//    Utils.runTraceAndPrintStats(deltaSnow,
//                                  (row: (String, Float)) => row._2 > 6000f,
//                                  input.map(_.value),
//                                  (s: String) => {
//                                    val arr = s.split(",")
//                                    arr(2).trim().equals("90in")
//                                  })
  }


  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    if( unit.equals("mm") ) return v else v * 304.8f
  }


  def zipToState(str: String): String = {
    (str.toInt % 50).toString
  }
}
