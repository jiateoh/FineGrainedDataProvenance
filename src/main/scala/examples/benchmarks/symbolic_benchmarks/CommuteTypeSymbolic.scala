package examples.benchmarks.symbolic_benchmarks

import org.apache.spark.{SparkConf, SparkContext}
import provenance.rdd.ProvenanceRDD._
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils
import symbolicprimitives.SymImplicits._

/**
  * Created by malig on 3/27/18.
  * Copied from BigTest by jteoh on 4/15/20: https://github
  * .com/maligulzar/BigTest/blob/JPF-integrated/benchmarks/src/subject/programs/CommuteType.scala
  */
object CommuteTypeSymbolic {
  
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
    // TODO: FIX THIS TO REFLECT INFLUENCE PROGRAM WHICH DOES NOT USE JOIN
    val tripLines = sc.textFileSymbolic("datasets/trips") //sc.parallelize(Array(data1(i)))
    val locationLines = sc.textFileSymbolic("datasets/zipcode") //sc.parallelize(Array(data2(i)))
    Utils.setUDFAwareDefaultValue(true)
    // For-loop removed
    // for(i <- 0 to data1.length-1){
    try{
      val trips = tripLines
                    .map { s =>
                      val cols = s.split(",")
                      // Technically these parseInt calls 'lose' provenance, but it's still
                      // retained in the first tuple element. Worth noting that Integer.parseInt
                      // is not the 'Scala' way either, we should be using cols(x).toInt instead
                      (cols(1), Integer.parseInt(cols(3)) / Integer.parseInt(cols(4)))
                    }
      val locations = locationLines
                        .map { s =>
                          val cols = s.split(",")
                          (cols(0), cols(1))
                        }
                        // jteoh: adjusted because datagen treats these as zip codes rather than
                        // neighborhood names. Also, column filter is wrong.
                        //.filter(s => s._2.equals("Palms"))
                        //.filter(s => s._1.equals("90034"))
      val joined = trips.join(locations)
      // TODO: The below map UDF does not retain symbolic objects, we lose
      //  provenance using the symbolic approach. This benchmark is thus not properly supported.
      println("WARNING: Symbolic approach for Commute Type benchmark is not properly supported " +
                "yet!")
      val types = joined
        .map { s =>
          // Checking if speed is < 25mi/hr
          if (s._2._1 > 40) {
            ("car", 1)
          } else if (s._2._1 > 15) {
            ("public", 1)
          } else {
            ("onfoot", 1)
          }
        }
        val out = types.reduceByKey(_ + _)
        out.collectWithProvenance()
        .foreach(println)
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
    //}
    
    println("Time: " + (System.currentTimeMillis() - startTime))
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
