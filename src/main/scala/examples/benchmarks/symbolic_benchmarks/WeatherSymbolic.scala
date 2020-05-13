package examples.benchmarks.symbolic_benchmarks

import examples.benchmarks.AggregationFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{MathSym, SymDouble, SymFloat, SymString, Utils}
import symbolicprimitives.SymImplicits._

/**
  * Created by ali on 2/25/17.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/weather/WeatherAnalysis.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object WeatherSymbolic {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Weather Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/weatherdata"
    } else {
      logFile = args(0)
      local = args(1).toInt
    }
  
    Utils.setUDFAwareDefaultValue(true) // Assumption: symbolic objects will be available as
    // reliable sources of provenance information throughout this application.
    // (this is primarily for the aggByKey)
    
    //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val scdp = new SparkContextWithDP(ctx)
    val input = scdp.textFileSymbolic(logFile)

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
      List[(SymString, SymFloat)](
        (monthdate, snow),
        (year, snow)
      ).iterator
    }
    
    //split.rdd.asInstanceOf[RDD[((SymString, SymFloat), Provenance)]].filter(_._2.count > 2)
    //     .take(100).foreach(println)
    //split.takeWithProvenance(10).foreach(println)
    //println("TADA!")
    //System.exit(1)
    
    val deltaSnow = AggregationFunctions.minMaxDeltaByKey(split)

    // arbitrary test function
    def testFn(row: (SymString, SymFloat)): Boolean =
      row._2 > 6000f
      //row._1.value == "30/1"
    
    val traceResults = Utils.debugAndTracePrints(deltaSnow, testFn,
                              input.filter(s => {
                                //s.contains("90in")
                                val arr = s.split(",")
                                arr(2).trim().equals("90in")
                              }).rdd,
                              input.rdd)
    println("Correct Trace count: " + traceResults.count(s => {
      val arr = s.split(",")
      arr(2).trim().equals("90in")
    }))
    println("------- ALL TRACE RESULTS ----------")
    traceResults.foreach(println)
    
    
         
//    val (outCollect, collectTime) = Utils.measureTimeMillis(deltaSnow.collectWithProvenance())
//    //outCollect.foreach(println)
//    outCollect.map(row => (row._1._1.value, row._1._2.value, row._2)).take(100).foreach(println)
//    println(s"TOTAL: ${outCollect.length}")
//    println(s"Collect time $collectTime")
    
  }


  def convert_to_mm(s: SymString): SymFloat = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    if( unit.equals("mm") ) return v else v * 304.8f
  }


  def zipToState(str: SymString): SymString = {
    (str.toInt % 50).toSymString
  }
}
