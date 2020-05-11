package examples.benchmarks.generators

import java.awt.image.ImageConsumer
import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by ali on 2/25/17.
  * jteoh copied from https://raw.githubusercontent
  * .com/maligulzar/BigTest/JPF-integrated/benchmarks/src/datagen/CommuterDataGen.scala
  * Formerly named CommuteDataGen
  */
object CommuteTypeDataGenerator {
  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()
    val random = new Random(42) // fixed seed for reproducability
    var logFile = ""
    var partitions = 100
    var dataper  = 500000
    val faultRate = 1.0 / 100000
    def shouldInjectFault(dis: Int, time: Int): Boolean = {
      (dis / time > 40) && random.nextDouble() <= faultRate
    }
    
    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("Commute_LineageDD").set("spark.executor.memory", "2g")
      logFile =  "datasets/commute/"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val trips = logFile + "trips"
    val zip = logFile + "zipcode"
  
    FileUtils.deleteQuietly(new File(trips))
    FileUtils.deleteQuietly(new File(zip))
    
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).map{_ =>
        def zipcode = "9" + "0"+ "0" + Random.nextInt(10).toString + Random.nextInt(10).toString
        var z1 = zipcode
        var z2 = zipcode
        val dis = Math.abs(z1.toInt - z2.toInt)*100 +  Random.nextInt(10)
        val time = Math.max(dis/(Random.nextInt(50)+10), 1) // time should be at least 1 to avoid
        // divide-by-zero error
        val adjustedDis = if(shouldInjectFault(dis, time)) {
          dis + (600 * time) // an extra 500 mph or so
        } else {
          dis
        }
        s"""sr,${z1},${z2},$adjustedDis,$time"""
        }.toIterator}.saveAsTextFile(trips)
    sc.textFile(trips).flatMap(s => Array(s.split(",")(1) , s.split(",")(2))).distinct().map(s =>s"""$s,${(s.toInt%100).toString}""" )
      .saveAsTextFile(zip)
  }
}