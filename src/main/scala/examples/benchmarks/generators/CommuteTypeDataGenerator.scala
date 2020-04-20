package examples.benchmarks.generators

import java.awt.image.ImageConsumer

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
    var logFile = ""
    var partitions = 2
    var dataper  = 50
    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("TermVector_LineageDD").set("spark.executor.memory", "2g")
      logFile =  "datasets/"
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    val trips = logFile + "trips"
    val zip = logFile + "zipcode"
    
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).flatMap{_ =>
        def zipcode = "9" + "0"+ "0" + Random.nextInt(10).toString + Random.nextInt(10).toString
        var z1 = zipcode
        var z2 = zipcode
        val dis = Math.abs(z1.toInt - z2.toInt)*100 +  Random.nextInt(10)
        val time = Math.max(dis/(Random.nextInt(50)+10), 1) // time should be at least 1 to avoid
        // divide-by-zero error
        var list = List[String]()
        list = s"""sr,${z1},${z2},$dis,$time""" :: list
        list}.iterator}.saveAsTextFile(trips)
    sc.textFile(trips).flatMap(s => Array(s.split(",")(1) , s.split(",")(2))).distinct().map(s =>s"""$s,${(s.toInt%100).toString}""" )
      .saveAsTextFile(zip)
  }
}