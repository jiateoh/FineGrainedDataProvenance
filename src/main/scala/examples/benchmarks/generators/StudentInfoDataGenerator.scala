package examples.benchmarks.generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object StudentInfoDataGenerator {

  var logFile = ""
  var partitions = 10
  var dataper  = 2500000
  var fault_rate = 0.000004
  val random = new Random(42)
  def shouldInjectFault(grade: String): Boolean = {
    grade == "Junior" && random.nextDouble() <= fault_rate
  }
  val genderList =  Array("male", "female")
  val collegeYear = Array("Freshman", "Sophomore", "Junior", "Senior")
  val majorList = Array("English", "Mathematics", "ComputerScience", "ElectricalEngineering", "Business", "Economics", "Biology",
  "Law", "PoliticalScience", "IndustrialEngineering")

  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()

    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("StudentInfoGenerator").set("spark.executor.memory", "2g")
      logFile =  "datasets/studentInfo"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val grades = logFile
    FileUtils.deleteQuietly(new File(grades))

   // fixed seed for reproducability
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map { _ =>
        val studentId = random.nextInt(99999999)
        val major = majorList(random.nextInt(majorList.length))

        val gender = genderList(random.nextInt(genderList.length))
        val cy = random.nextInt(collegeYear.length)
        val year =  collegeYear(cy)
        val insert_fault = shouldInjectFault(year)
        val age = if (insert_fault){
          println("Injecting")
          studentId }else random.nextInt(6) + 15+ cy*2 //Each year has a consistent average age (17,19,21,23)
        val str = s"$studentId,$major,$gender,$year,$age"
        str
      }.toIterator
    }.saveAsTextFile(grades)

    println(s"Wrote file to $grades")
  }
}
