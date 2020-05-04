package examples.benchmarks.symbolic_benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymInt, Utils}
import symbolicprimitives.SymImplicits._

/**
  * Created by Michael on 4/14/16.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/StudentInfo.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object StudentInfoSymbolic {
  
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    var logFile = ""
    if(args.isEmpty){
      sparkConf.setAppName("Student_Info")
               .set("spark.executor.memory", "2g").setMaster("local[6]")
      logFile = "datasets/student.txt"
      // https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/datageneration/student.txt
    }else{
      logFile = args(0)
    }
    //set up spark context
    val ctx = new SparkContext(sparkConf)
    
    //set up lineage context
    val scdp = new SparkContextWithDP(ctx)
    
    
    val records = scdp.textFileSymbolic(logFile)
    Utils.setUDFAwareDefaultValue(true)
    
    val grade_age_pair = records.map(line => {
      val list = line.split(" ")
      (list(4).toInt, list(3).toInt)
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
    val average_age_by_grade = grade_age_pair.aggregateByKey[(SymLong, SymInt)]((0L, 0), 4)(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)},
      enableUDFAwareProv = Some(true), // redundant as this is already set to true at app start.
      influenceTrackerCtr = None) // need to provide default values due to API limitations.)
    .mapValues({case (sum, count) => sum.toDouble / count})
    //val out = average_age_by_grade.collect()
    //out.foreach(println)
    val out = average_age_by_grade.collectWithProvenance()
    println("((Grade, Age), Provenance)")
    out.foreach(println)
    
    // REMOVED: print out the result for debugging purpose
    
    // REMOVED: getLineage and tracing
    
    println("Job's DONE!")
    ctx.stop()
    
  }
  
}
