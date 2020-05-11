package examples.benchmarks.generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object StudentInfoDataGenerator {

  var logFile = ""
  var partitions = 10
  var dataper  = 2500000
  var fault_rate = 0.000001
  val random = new Random(42)
  def faultInjector()  = if(random.nextInt(dataper*partitions) < dataper*partitions* fault_rate) true else false

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

        val insert_fault = faultInjector()
        val studentId = random.nextInt(99999999)
        val major = majorList(random.nextInt(majorList.length))
        val gender = genderList(random.nextInt(genderList.length))
        val cy = random.nextInt(collegeYear.length)
        val year = if(insert_fault) collegeYear(2) else collegeYear(cy)
        val age = if (insert_fault) studentId else random.nextInt(6) + 15+ cy*2
        val str = s"$studentId,$major,$gender,$year,$age"
        str
      }.toIterator
    }.saveAsTextFile(grades)

    println(s"Wrote file to $grades")
  }
}
