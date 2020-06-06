package examples.benchmarks.influence_benchmarks

import examples.benchmarks.AggregationFunctions
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.{FloatStreamingOutlierInfluenceTracker, StreamingOutlierInfluenceTracker}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymFloat, SymString, Utils}

/** Temporary program, used to reflect example in the FlowDebug paper */
object RunningExample2 {
  
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
    
    
    //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val scdp = new SparkContextWithDP(ctx)
    val input = scdp.textFileProv(logFile)
  
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
    val snowVariance = split.aggregateByKey((0.0,0.0, 0))(
      { case ((sum, sum2, count), next) => (sum + next,sum2 + next*next, count + 1) },
      { case ((sum11,sum12, count11), (sum21,sum22, count22)) => (sum11 + sum21, sum22+sum12 ,
        count11 + count22)},
       enableUDFAwareProv = Some(false),
       influenceTrackerCtr = Some(() => FloatStreamingOutlierInfluenceTracker())
                                                                                     ).mapValues({ case (sum,sum2, count) => ( (count*sum2.toFloat) - (sum.toFloat*sum.toFloat))/(count*(count-1)) })
    
    
    // output test function
    def testFn(row: (String, Float)): Boolean =
      row._2 > 6000f
    
    def faultFn(s: String): Boolean = {
      val arr = s.split(",")
      arr(2).trim().equals("90in")
    }
    // is there a particular output record in mind for tracing?
    Utils.runTraceAndPrintStats(snowVariance,
                                (row: (String, Float)) => row._2 > 6000f,
                                input,
                                (s: String) => {
                                  val arr = s.split(",")
                                  arr(2).trim().equals("90in")
                                })
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
