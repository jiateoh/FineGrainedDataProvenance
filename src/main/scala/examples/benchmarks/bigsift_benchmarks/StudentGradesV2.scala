package examples.benchmarks.bigsift_benchmarks

import examples.benchmarks.bigsift.BigSift
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.lineage.ui.BSListenerBusImpl
import org.apache.spark.rdd.RDD

object StudentGradesV2 {
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
    bs.runWithBigSift[(String, (Double, Double))](app, Some(failure))
    bs.getDebuggingStatistics()
  }

  def app(input: RDD[String], lineage: Lineage[String]): RDD[(String, (Double, Double))] = {
    if (lineage != null) {
      fun(lineage)
    } else {
      fun(input)
    }
  }

  def fun(input: Lineage[String]): RDD[(String, (Double, Double))] = {
    val courseGrades = input.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      (courseId, grade)
    })
    val courseGpas = courseGrades.mapValues(grade => {
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
    })
    val courseGpaAvgs= courseGpas.aggregateByKey((0.0, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})

    val deptGpas = courseGpaAvgs.map({case (courseId, gpa) =>
      val dept = courseId.split("\\d", 2)(0).trim()
      (dept, gpa)
    })

    val deptGpaStats = deptGpas.aggregateByKey((0.0, 0.0, 0.0))({
      case (agg, newValue) =>
        var (count, mean, m2) = agg
        count += 1
        val delta  = newValue - mean
        mean += delta / count
        val delta2 = newValue - mean
        m2 += delta * delta2
        (count, mean, m2)
    }, {
      case (aggA, aggB) =>
        val (countA, meanA, m2A) = aggA
        val (countB, meanB, m2B) = aggB
        val count = countA + countB
        val delta = meanB - meanA
        val mean = meanA + delta * countB / count
        val m2 = m2A + m2B + (delta * delta) * (countA * countB / count)
        (count, mean, m2)
    }
    )

    val deptGpaMeanVar = deptGpaStats.mapValues({case (count, mean, m2) =>
      (mean, m2 / count)
    })

    deptGpaMeanVar
  }
  def fun(input: RDD[String]): RDD[(String, (Double, Double))] = {
    val courseGrades = input.map(line => {
      val arr = line.split(",")
      val (courseId, grade) = (arr(1), arr(2).toInt)
      (courseId, grade)
    })
    val courseGpas = courseGrades.mapValues(grade => {
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
    })
    val courseGpaAvgs= courseGpas.aggregateByKey((0.0, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})

    val deptGpas = courseGpaAvgs.map({case (courseId, gpa) =>
      val dept = courseId.split("\\d", 2)(0).trim()
      (dept, gpa)
    })

    val deptGpaStats = deptGpas.aggregateByKey((0.0, 0.0, 0.0))({
      case (agg, newValue) =>
        var (count, mean, m2) = agg
        count += 1
        val delta  = newValue - mean
        mean += delta / count
        val delta2 = newValue - mean
        m2 += delta * delta2
        (count, mean, m2)
    }, {
      case (aggA, aggB) =>
        val (countA, meanA, m2A) = aggA
        val (countB, meanB, m2B) = aggB
        val count = countA + countB
        val delta = meanB - meanA
        val mean = meanA + delta * countB / count
        val m2 = m2A + m2B + (delta * delta) * (countA * countB / count)
        (count, mean, m2)
    }
    )

    val deptGpaMeanVar = deptGpaStats.mapValues({case (count, mean, m2) =>
      (mean, m2 / count)
    })

    deptGpaMeanVar
  }

  def failure(record: (String, (Double, Double))): Boolean = {
    record._2._2 > 0.96f
  }
}

