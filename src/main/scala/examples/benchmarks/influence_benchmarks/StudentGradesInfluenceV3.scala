package examples.benchmarks.influence_benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.RoaringBitmapProvenance
import provenance.rdd.{BottomNInfluenceTracker, FilterInfluenceTracker, StreamingOutlierInfluenceTracker, TopNInfluenceTracker, UnionInfluenceTracker}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils


object StudentGradesInfluenceV3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    var logFile = ""
    conf.setAppName("StudentGradesV2")

    if (args.length < 2) {
      conf.setMaster("local[*]")
      conf.setAppName("Student GradesV3").set("spark.executor.memory", "2g")
      logFile = "datasets/studentGradesV2"
    } else {
      logFile = args(0)
      //local = args(1).toInt
    } //set up spark context

    val sc = new SparkContext(conf)
    val scdp = new SparkContextWithDP(sc)
    val lines = scdp.textFileProv(logFile)

    val out = lines.map(line => {
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
    }).aggregateByKey((0.0, 0.0, 0))(
      { case ((sum, sum2, count), next) => (sum + next, sum2 + next * next, count + 1) },
      { case ((sum11, sum12, count11), (sum21, sum22, count22)) => (sum11 + sum21, sum22 + sum12, count11 + count22)},
      enableUDFAwareProv = Some(false),
      influenceTrackerCtr = Some(() => StreamingOutlierInfluenceTracker(zscoreThreshold = 0.96))
    ).mapValues({ case (sum, sum2, count) => (sum2.toDouble - (sum.toDouble * sum.toDouble / count)) / count })

    val elapsed = Utils.measureTimeMillis({
      val outCollect = out.collectWithProvenance()
      println("Department, Variance")
      outCollect.foreach(println)

      // Debugging
      val csRecord = outCollect.filter(_._1._1 == "CS").head // get the CS row
      val csProvenance = csRecord._2
      val trace = Utils.retrieveProvenance(csProvenance)
      println("----- TRACE RESULTS ------")
      println("Count = " + trace.count())
      //trace.take(100).foreach(println)
    })
    println(s"Elapsed time: $elapsed ms")
  }
}
