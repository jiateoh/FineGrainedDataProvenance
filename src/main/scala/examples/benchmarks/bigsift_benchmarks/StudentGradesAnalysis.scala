package examples.benchmarks.bigsift_benchmarks

import examples.benchmarks.bigsift.BigSift
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.lineage.ui.BSListenerBusImpl
import org.apache.spark.rdd.RDD

object StudentGradesAnalysis {
  var bus: BSListenerBusImpl = null

  def main(args: Array[String]): Unit = {
    //set up spark configuration
    val sparkConf = new SparkConf()
    var logFile = ""
    var local = 500
    if (args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Student Grade Analysis").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesV2/*"
    } else {
      logFile = args(0)
      local = args(1).toInt
    } //set up spark context
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    val bs = new BigSift(ctx, logFile)
    bs.runWithBigSift[(String, Double)](app, Some(failure))
    bs.getDebuggingStatistics()
  }

  def app(input: RDD[String], lineage: Lineage[String]): RDD[(String, Double)] = {
    if (lineage != null) {
      fun(lineage)
    } else {
      fun(input)
    }
  }

  def fun(input: RDD[String]): RDD[(String, Double)] = {
    input.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      (courseId.split("\\d", 2)(0).trim(), grade)
    }).mapValues(grade => {
      // https://pages.collegeboard.org/how-to-convert-gpa-4.0-scale
      if (grade >= 93) 4.0
      else if (grade >= 90) 3.7
      else if (grade >= 87) 3.3
      else if (grade >= 83) 3.0
      else if (grade >= 80) 2.7
      else if (grade >= 77) 2.3
      else if (grade >= 73) 2.0
      else if (grade >= 70) 1.7
      else if (grade >= 67) 1.3
      else if (grade >= 65) 1.0
      else 0.0
    }).aggregateByKey((0.0,0.0, 0))(
      { case ((sum, sum2, count), next) => (sum + next,sum2 + next*next, count + 1) },
      { case ((sum11,sum12, count11), (sum21,sum22, count22)) => (sum11 + sum21, sum22+sum12 ,count11 + count22)}
    ).mapValues({ case (sum,sum2, count) => ( sum2.toDouble - (sum.toDouble*sum.toDouble / count))/count })
  }

  def fun(input: Lineage[String]): RDD[(String, Double)] = {
    input.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      (courseId.split("\\d", 2)(0).trim(), grade)
    }).mapValues(grade => {
      // https://pages.collegeboard.org/how-to-convert-gpa-4.0-scale
      if (grade >= 93) 4.0
      else if (grade >= 90) 3.7
      else if (grade >= 87) 3.3
      else if (grade >= 83) 3.0
      else if (grade >= 80) 2.7
      else if (grade >= 77) 2.3
      else if (grade >= 73) 2.0
      else if (grade >= 70) 1.7
      else if (grade >= 67) 1.3
      else if (grade >= 65) 1.0
      else 0.0
    }).aggregateByKey((0.0,0.0, 0))(
      { case ((sum, sum2, count), next) => (sum + next,sum2 + next*next, count + 1) },
      { case ((sum11,sum12, count11), (sum21,sum22, count22)) => (sum11 + sum21, sum22+sum12 ,count11 + count22)}
    ).mapValues({ case (sum,sum2, count) => ( sum2.toDouble - (sum.toDouble*sum.toDouble / count))/count })
  }


  def failure(record: (String, Double)): Boolean = {
    record._2 > 0.96f
  }
}
