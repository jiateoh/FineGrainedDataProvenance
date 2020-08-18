package examples.benchmarks.baseline

import examples.benchmarks.AggregationFunctions
import org.apache.spark.{SparkConf, SparkContext}
import symbolicprimitives.Utils

/**
  * Created by Michael on 4/14/16.
  * Copied from BigSiftUI repo by jteoh on 4/16/20
  * https://github.com/maligulzar/BigSiftUI/blob/master/src/benchmarks/studentdataanalysis/StudentInfo.scala
  * Logging and other miscellaneous bigsift-specific functionality is removed.
  */
object StudentInfoBaseline {
  
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
    val sc = new SparkContext(sparkConf)
    
    val records = sc.textFile(logFile)
    
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
    // TODO: previous impl used 4 partitions, but this is removed. If anything this should make
    //  for worse performance, but rerun perf numbers if needed.
    val average_age_by_grade = grade_age_pair.aggregateByKey((0.0, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                                                          ).mapValues({case (sum, count) => sum.toDouble/count})
  
    Utils.runBaseline(average_age_by_grade)
    
    
  }
  
}
