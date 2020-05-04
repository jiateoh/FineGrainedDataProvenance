package examples.benchmarks.generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Adapted from StudentGradesDataGenerator
  * New Schema consists of: StudentID,CourseID,Grade
  */
object StudentGradesDataGeneratorV2 {
  
  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()
    var logFile = ""
    var partitions = 10
    var dataper  = 50000
    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("StudentGradesGenerator").set("spark.executor.memory", "2g")
      logFile =  "datasets/studentGradesV2"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val grades = logFile
    FileUtils.deleteQuietly(new File(grades))
    
    
    
    val depts = Seq("EE", "CS", "MATH", "Physics ", "STATS")
    val courseNums = Seq(0,100).flatMap(x => (1 to 20).map(_ + x))
    val courses = depts.flatMap(dept => courseNums.map(dept + _))
    
    val random = new Random(42) // fixed seed for reproducability
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map { _ =>
  
        val studentId = random.nextInt(190) + 10
        //val courseNum = (Random.nextInt(570) + 30).toString
        //val uppderDiv = Random.nextBoolean()
//        val courseNum = if(uppderDiv) Random.nextInt(100) + 200
//                        else Random.nextInt(30) + 30 // 30
        // courses
        //val course = s"$studentId,$courseNum"
        val course = courses(random.nextInt(courses.length))
        //val dept = if(Random.nextBoolean()) "EE" else "CS"
        
        // give CS courses more variance (note we don't actually care about the mean)
        // very low probability
        //        val grade = if(course.startsWith("CS") && random.nextDouble() < 0.01) {
        //          random.nextInt(20) // give them a fairly bad grade
        //        } else {
        //          random.nextInt(35) + 65
        //        }
        
        // new strat: if course is CS11 then give everybody A's or very high score at least.
        val grade = if(course == "CS11") {
          random.nextInt(10) + 90
        } else {
          random.nextInt(35) + 65
        }
        val str = s"$studentId,$course,$grade"
        str
      }.toIterator
    }.saveAsTextFile(grades)
    
    println(s"Wrote file to $grades")
  }
}
