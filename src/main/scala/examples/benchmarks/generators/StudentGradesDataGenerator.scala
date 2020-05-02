package examples.benchmarks.generators

import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by malig on 5/14/18.
  * Copied by jteoh and adapted on 4/27/20 from
  * https://github.com/maligulzar/BigTest/blob/JPF-integrated/BenchmarksFault/src/datagen/StudentGradeDataGen.scala
  */
object StudentGradesDataGenerator {
  
  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()
    var logFile = ""
    var partitions = 2
    var dataper  = 50
    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("StudentGradesGenerator").set("spark.executor.memory", "2g")
      logFile =  "datasets/studentGrades"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val grades = logFile
    
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map { _ =>
        
        val course= (Random.nextInt(570) + 30).toString
        val f = if(Random.nextBoolean()) "EE" else "CS"
        val students = Random.nextInt(190) + 10
        var list_std = List[Int]()
        for(i <- 0 to students){
          list_std  =  (Random.nextInt(90)+10) :: list_std
        }
        val str = list_std.map(s => f+course +":"+s).reduce(_+","+_)
        str
      }.toIterator
    }.saveAsTextFile(grades)
  }
}
