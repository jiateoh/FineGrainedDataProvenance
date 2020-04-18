package examples.benchmarks

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import sparkwrapper.SparkContextWithDP
import provenance.rdd.ProvenanceRDD._
import symbolicprimitives.{MathSym, SymFloat, SymString}

/**
  * Created by ali on 2/25/17.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/weather/WeatherAnalysis.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object Weather {

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Weather Analysis").set("spark.executor.memory", "2g")
      logFile = "weatherdata"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val scdp = new SparkContextWithDP(ctx)
    val input = scdp.textFileProv(logFile)


    val split = scdp.textFileProv("weather_data").flatMap { s =>
      val tokens = s.split(",")
      // finds the state for a zipcode
      var state = zipToState(tokens(0))
      var date = tokens(1)
      // gets snow value and converts it into millimeter
      val snow = convert_to_mm(tokens(2))
      //gets year
      val year = date.substring(date.lastIndexOf('/'))
      // gets month / date
      val monthdate = date.substring(0, date.lastIndexOf('/') - 1)
      List[(SymString, SymFloat)](
        (monthdate, snow),
        (year, snow)
      ).iterator
    }

    val zero = (SymFloat(Float.MaxValue, Provenance.provenanceFactory.create(-1)),
      SymFloat(Float.MinValue ,Provenance.provenanceFactory.create(-1)))

    val deltaSnow = split.aggregateByKey(zero)(
      {case ((curMin , curMax), next) => (MathSym.min(curMin, next), MathSym.max(curMax, next))},
      {case ((minA, maxA), (minB, maxB)) =>(MathSym.min(minA, minB), MathSym.max(maxA, maxB))})

      .mapValues({
        case (min, max) => max - min}
      )

    val out = deltaSnow.collectWithProvenance()
    out.foreach(println)


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
