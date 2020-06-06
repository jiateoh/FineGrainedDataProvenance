package examples.benchmarks.influence_benchmarks

import examples.benchmarks.AggregationFunctions
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.InfluenceMarker
import provenance.rdd.{IntStreamingOutlierInfluenceTracker, MaxInfluenceTracker, ProvenanceRDD, StreamingOutlierInfluenceTracker}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils

/**
  * Created by Michael on 4/14/16.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/StudentInfo.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object StudentInfoInfluence {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var logFile = ""
    if(args.isEmpty){
      sparkConf.setAppName("Student_Info")
               .set("spark.executor.memory", "2g").setMaster("local[6]")
      logFile = "datasets/studentInfo"
      // https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/datageneration/student.txt
    }else{
      logFile = args(0)
    }
    //set up spark context
    val ctx = new SparkContext(sparkConf)
    
    //set up lineage context
    val scdp = new SparkContextWithDP(ctx)
    
    
    val records = scdp.textFileProv(logFile)
    
    val grade_age_pair = records.map(line => {
      val list = line.split(",")
      (list(3), list(4).toInt)
    })
    
    /** val average_age_by_grade = grade_age_pair.groupByKey
                                             .map(pair => {
                                               val itr = pair._2.toIterator
                                               var moving_average = 0.0
                                               var num = 1
                                               while (itr.hasNext) {
                                                 moving_average = moving_average + (itr.next() - moving_average) / num
                                                 num = num + 1
                                               }
                                               (pair._1, moving_average)
                                             })**/
    // Because this program currently defines 4 partitions, we don't have an explicit
    // AggregationFunction UDF for it.
    val average_age_by_grade = AggregationFunctions.averageByKeyWithInfluence(grade_age_pair)
  
    Utils.runTraceAndPrintStats(average_age_by_grade,
                                  (row: (String, Double)) => row._2 > 30,
                                  records,
                                  (line: String) => {
                                    val list = line.split(",")
                                    list(4).toInt > 30
                                  })
    ctx.stop()
    
  }
  
}
