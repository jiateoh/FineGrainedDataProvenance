package examples.benchmarks.influence_benchmarks

import examples.benchmarks.AggregationFunctions
import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD._
import provenance.rdd.{IntStreamingOutlierInfluenceTracker, StreamingOutlierInfluenceTracker, TopNInfluenceTracker}
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils

/**
  * Created by malig on 3/27/18.
  * Copied from BigTest by jteoh on 4/15/20: https://github
  * .com/maligulzar/BigTest/blob/JPF-integrated/benchmarks/src/subject/programs/CommuteType.scala
  */
object CommuteTypeInfluence {
  
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("CommuteTime")
    
    
    val data1 = Array(",, ,0,1",
                      ",, ,16,1",
                      ",, ,41,1",
                      " , , ,",
                      " , , , ,0",
                      " , , , ,",
                      "","","",
                      ",A, ,-0,1",
                      ",A, ,-0,1")
    
    val data2 = Array(",Palms",
                      ",Palms",
                      ",Palms",
                      "",
                      "",
                      "",
                      "",
                      ",",
                      ",",
                      "",
                      "")
    
    val startTime = System.currentTimeMillis();
    val _sc = new SparkContext(conf)
    val sc = new SparkContextWithDP(_sc)
    val tripLines = sc.textFileProv("datasets/commute/trips") //sc.parallelize(Array(data1(i)))
    try{
      val trips = tripLines
                    .map { s =>
                      val cols = s.split(",")
                      (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
                    }
      val types = trips
        .map { s =>
          val speed = s._2
          if (speed > 40) {
            ("car", speed)
          } else if (speed > 15) {
            ("public", speed)
          } else {
            ("onfoot", speed)
          }
        }
        
      //val out = AggregationFunctions.sumByKey(types)// types.reduceByKey(_ + _)
      val out = AggregationFunctions.averageByKey(types,
                                                  enableUDFAwareProv = Some(false),
                                                  influenceTrackerCtr = Some(() =>
                                                                             TopNInfluenceTracker(1000)))
                                                                             //IntStreamingOutlierInfluenceTracker(zscoreThreshold = 10)))
      Utils.runTraceAndPrintStats(out,
                                    (row: (String, Double)) => row._1 == "car",
                                    tripLines,
                                    (s: String) => {
                                      val cols = s.split(",")
                                      Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)) > 500
                                    })
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    //}
    
    //println("Time: " + (System.currentTimeMillis() - startTime))
    //    val trips = sc
    //      .textFile(
    //        "/Users/malig/workspace/up_jpf/benchmarks/src/datasets/trips/*")
    //      .map { s =>
    //        val cols = s.split(",")
    //        (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
    //      }
    //    val locations = sc
    //      .textFile(
    //        "/Users/malig/workspace/up_jpf/benchmarks/src/datasets/zipcode/*")
    //      .map { s =>
    //        val cols = s.split(",")
    //        (cols(0), cols(1))
    //      }
    //      .filter(s => s._2.equals("34"))
    //    val joined = trips.join(locations)
    //    joined
    //      .map { s =>
    //        // Checking if speed is < 25mi/hr
    //        if (s._2._1 > 40) {
    //          ("car", 1)
    //        } else if (s._2._1 > 15) {
    //          ("public", 1)
    //        } else {
    //          ("onfoot", 1)
    //        }
    //      }
    //      .reduceByKey(_ + _)
    //      .collect
    //      .foreach(println)
    
  }
}
