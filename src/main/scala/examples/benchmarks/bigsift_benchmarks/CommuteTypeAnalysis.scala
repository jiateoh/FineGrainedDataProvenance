package examples.benchmarks.bigsift_benchmarks

import examples.benchmarks.bigsift.BigSift
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.ui.BSListenerBusImpl
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CommuteTypeAnalysis {
  var bus: BSListenerBusImpl = null

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Student Age Analysis").set("spark.executor.memory", "2g")
      //logFile = "datasets/commute/trips"
      logFile = "datasets/commute/trips/part-000[0-4]*" //halving for program stability
    } else {
      logFile = args(0)
    }
    val ctx = new SparkContext(sparkConf)
    val bsift = new BigSift(ctx, logFile)
    bsift.runWithBigSift[(String, Double)](app,Some(failure))
    bsift.getDebuggingStatistics()
  }

  def app(input: RDD[String] , lineage: Lineage[String]): RDD[(String, Double)] = {
    if(lineage!= null){
      program(lineage)
    }else{
      program(input)
    }
  }

  def program (input: RDD[String] ): RDD[(String, Double)] = {
      val types = input.map { s =>
          val cols = s.split(",")
          (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
        }.map { s =>
          val speed = s._2
          if (s._2 > 40) {
            ("car", speed)
          } else if (s._2 > 15) {
            ("public", speed)
          } else {
            ("onfoot", speed)
          }
        }

      val out = types.aggregateByKey((0L, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
        ).mapValues({case (sum, count) => sum.toDouble/count})
    out
    }

  def program (input: Lineage[String] ): RDD[(String, Double)] = {
    val types = input.map { s =>
      val cols = s.split(",")
      (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
    }.map { s =>
      val speed = s._2
      if (s._2 > 40) {
        ("car", speed)
      } else if (s._2 > 15) {
        ("public", speed)
      } else {
        ("onfoot", speed)
      }
    }

    val out = types.aggregateByKey((0L, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})
    out
  }

  def failure(record: (Any, Double)): Boolean = {
    val r = record.asInstanceOf[(String, Double)]
    r._1 == "car" && r._2 > 50
  }

}
