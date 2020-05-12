package examples.benchmarks.generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by ali on 2/25/17.
  * Copied from BigSiftUI by jteoh on 4/17/20
  */
object WeatherDataGenerator {
  var logFile = ""
  var partitions = 10
  var dataper  = 100
  var fault_rate = 0.000001
  val random = new Random(42)
  def main(args:Array[String]) =
  {
    val sparkConf = new SparkConf()

    if(args.length < 2) {
      sparkConf.setMaster("local[6]")
      sparkConf.setAppName("TermVector_LineageDD")//.set("spark.executor.memory", "2g")
      logFile =  "datasets/weatherdata"
      //logFile = args(0)
    }else{
      logFile = args(0)
      partitions =args(1).toInt
      dataper = args(2).toInt
    }
    FileUtils.deleteQuietly(new File(logFile))
    val sc = new SparkContext(sparkConf)
    sc.parallelize(Seq[Int]() , partitions).mapPartitions { _ =>
      (1 to dataper).flatMap{_ =>
        var zipcode = (random.nextInt(9)+1).toString + random.nextInt(10).toString + random.nextInt(10).toString + random.nextInt(10).toString + random.nextInt(10).toString
        var list = List[String]()
        for(day <- 1 to 30){
          for(m <- 1 to 12){
            for( y <- 1900 to 2016){
              var snow = "0mm"
              if(zipcode.startsWith("3") || zipcode.startsWith("7") || zipcode.startsWith("9")){
                snow = getLowSnow()
                list = s"""$zipcode,$day/$m/$y,$snow""" :: list
              }else{
                snow = getHighSnow()
                list = s"""$zipcode,$day/$m/$y,$snow""" :: list
              }
            }
          }
        }
        list}.iterator}.saveAsTextFile(logFile)
    
    
  }


  def faultInjector(): Boolean ={
    if(random.nextInt(dataper *30*12*116* partitions) < dataper *30*12*116* partitions*fault_rate)
     true else false
  }
  def getLowSnow(): String = {
    if(faultInjector()){
      return  "90in"
    }
    if(random.nextInt(2) == 0){
      return random.nextInt(160) + "mm"
    }else{
      return (random.nextFloat()/2) + "ft"
    }
  }
  def getHighSnow(): String ={
    if(faultInjector()){
      return  "90in"
    }
    if(random.nextInt(2) == 0){
      return random.nextInt(4000) + "mm"
    }else{
      return (random.nextFloat()*13) + "ft"
    }
  }
}
//2,088,000
