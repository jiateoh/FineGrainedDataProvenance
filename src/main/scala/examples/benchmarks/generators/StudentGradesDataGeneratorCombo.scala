package examples.benchmarks.generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Adapted from StudentGradesDataGenerator
  * New Schema consists of: StudentID,CourseID,Grade
  */
object StudentGradesDataGeneratorCombo {
  
  var logFile = ""
  var partitions = 10
  var dataper  = 5000000 // 500000 before
  val depts = Seq("EE", "CS", "MATH", "Physics ", "STATS")
  val courseNums = Seq(0,100).flatMap(x => (1 to 99).map(_ + x)) //(basically 1 -> 199)
  val faultTargetCourses = Seq("CS9", "CS11")//, "CS14", "CS17") // (note: there are 5 dept x
  // 80 and 50M rows, so 2 courses should equal 250K records.
  // course numbers each for a total of 200, so this represents a small fraction of total records)
  
  // UNUSED
  def shouldInjectFault(course: String): Boolean = faultTargetCourses.contains(course)// && Random.nextDouble() <= faultRate
  // UNUSED
  def injectFault(course: String): String = {
    // TODO figure out the desired fault - maybe it's just that all entries are lower?
    // check original program for better ideas...
    if(course == "CS9") {
      // do something
      ""
    } else if (course == "CS11") {
      // do something else
      ""
    }
    ""
  }
  
  // Add a special class (outside typical range) with 45 records, one of which is a large typo
  // that skews the average above 100
  def addExtraFaults(sc: SparkContext, random: Random): RDD[String] = {
    // add an extra, small course with *mostly* OK values, but one abnormally high typo
    // rough math: (899 + (44 * 82.5)) / 45 yields a ~100.6 average, very high and enough to skew
    // the top/bottomN values
    val singleFaultClass = (1 to 45).map(x => {
      val studentId = random.nextInt(190) + 10
      val course = "CS301"
      val grade = if(x == 7) {
        899 // a 'typo' where last digit is repeated
      } else {
        random.nextInt(15) + 75
      }
      s"$studentId,$course,$grade"
    })
    val singleFaultClassRDD = sc.parallelize(singleFaultClass)
    
    // For two additional classes, create an abnormally low average by zeroing out some scores.
    // the number of zeroes should be higher than the influence function capacity used to track
    // them.
    // with 100 students and a 15% rate, that's about 2x 15 students for a total of 30.
    val lowAvgClasses = Seq("Physics 401", "Physics 402").flatMap(course => {
      (1 to 100).map(_ => {
        val studentId = random.nextInt(190) + 10
        // roughly 15% can be zeroed out - with an average of 8.25, this drops the average
        // significantly down to about 70%
        val grade = if (random.nextDouble() < 0.15) {
          0
        } else {
          random.nextInt(35) + 65
        }
        s"$studentId,$course,$grade"
      })
    })
    val lowAvgClassesRDD = sc.parallelize(lowAvgClasses)
    
    val faults = singleFaultClassRDD.union(lowAvgClassesRDD)
    faults
  }
  
  def isFault(str: String): Boolean = {
    val split = str.split(",")
    val course = split(1)
    val grade = split(2)
    (course == "CS301" && grade == "899") ||
      ((course == "Physics 401" || course == "Physics 402") && grade == "0")
  }
  
  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()
    val random = new Random(42) // fixed seed for reproducability
    
    
    
    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("StudentGradesGenerator").set("spark.executor.memory", "2g")
      logFile =  "datasets/studentGradesCombo"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val grades = logFile
    FileUtils.deleteQuietly(new File(grades))
    
    
    
    val courses = depts.flatMap(dept => courseNums.map(dept + _))
    
    val sc = new SparkContext(sparkConf)
    val data = sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map { _ =>
  
        val studentId = random.nextInt(190) + 10
        val course = courses(random.nextInt(courses.length))
        // commented out for combo generator - adding faults as an add-on instead.
        /*val grade = if(shouldInjectFault(course)) {
          //random.nextInt(45) + 20
          //random.nextInt(25) + 40
          injectFault(course)
        } else {
          random.nextInt(35) + 65
        }*/
        val grade = random.nextInt(35) + 65
        val str = s"$studentId,$course,$grade"
        str
      }.toIterator
    }
    
    val extraFaults = addExtraFaults(sc, random)
    
    data.union(extraFaults).saveAsTextFile(grades)
    
    println(s"Wrote file to $grades")
  }
}
