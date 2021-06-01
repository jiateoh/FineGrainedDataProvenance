package examples.benchmarks.bigsift_benchmarks

import examples.benchmarks.bigsift.BigSift
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.ui.BSListenerBusImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WeatherComboTest {
  var bus: BSListenerBusImpl = null

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Weather Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/weatherdata/part-00000"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val bs = new BigSift(ctx , logFile)
    bs.runWithBigSift[(String, Float)](app, Some(failure))
    bs.getDebuggingStatistics()
  }

  def app(input: RDD[String], lineage: Lineage[String]): RDD[(String, Float)] = {
    if (lineage != null) {
      fun(lineage)
    } else {
      fun(input)
    }
  }

  def fun(input: RDD[String]): RDD[((String, Float))] = {
    val split = input.map { s =>
    //val split = input.flatMap { s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      //val year = date.substring(date.lastIndexOf("/"))
      // gets month / date
      //val monthdate = date.substring(0, date.lastIndexOf("/") - 1)
//      List[(String, Float)](
//        (monthdate, snow),
//        (year, snow)
//      ).iterator
      ((state, date), snow)
    }
//    val deltaSnow = split.aggregateByKey((Float.MaxValue, Float.MinValue))(
//      { case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next)) },
//      { case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB)) })
//      .mapValues({ case (min, max) => max - min })
    
    val stateDateSums = split.reduceByKey(_+_)
    val stateMax = stateDateSums.map({case ((state, date), snow) => (state, snow) })
                                .reduceByKey(Math.max)
    stateMax
  }

  def fun(input: Lineage[String]): RDD[((String, Float))] = {
    val split = input.map { s =>
      //val split = input.flatMap { s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      //val year = date.substring(date.lastIndexOf("/"))
      // gets month / date
      //val monthdate = date.substring(0, date.lastIndexOf("/") - 1)
      //      List[(String, Float)](
      //        (monthdate, snow),
      //        (year, snow)
      //      ).iterator
      ((state, date), snow)
    }
    //    val deltaSnow = split.aggregateByKey((Float.MaxValue, Float.MinValue))(
    //      { case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next)) },
    //      { case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB)) })
    //      .mapValues({ case (min, max) => max - min })
  
    val stateDateSums = split.reduceByKey(_+_)
    val stateMax = stateDateSums.map({case ((state, date), snow) => (snow.toString, snow) }) // TODO
                                // rekeying for testing only
                                .reduceByKey(Math.max)
    stateMax
  }


  def convert_to_mm(s: String): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    unit match {
      case "mm" => return v
      case _ => return v * 304.8f
    }
  }

  def failure(record: (String, Float)): Boolean = {
    record.asInstanceOf[(String, Float)]._2 > 10000f
  }

  def zipToState(str: String): String = {
    return (str.toInt % 50).toString
  }

}
