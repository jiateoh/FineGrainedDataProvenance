package examples.benchmarks.baseline

import org.apache.spark.{SparkConf, SparkContext}
import symbolicprimitives.Utils

/**
  * Created by malig on 3/27/18.
  * Copied from BigTest by jteoh on 4/15/20: https://github
  * .com/maligulzar/BigTest/blob/JPF-integrated/benchmarks/src/subject/programs/CommuteType.scala
  */
object CommuteTypeBaseline {
  
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
    val sc = new SparkContext(conf)
    //val tripLines = sc.textFileProv("datasets/commute/trips") //sc.parallelize(Array(data1(i)))
    // halving dataset size for program stability in Titian/BigSift
    val tripLines = sc.textFile("datasets/commute/trips/part-000[0-4]*")
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
      // other functions to consider: intstreaming
      val out = types.aggregateByKey((0.0, 0))(
        {case ((sum, count), next) => (sum + next, count+1)},
        {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                                                            ).mapValues({case (sum, count) => sum.toDouble/count})
      Utils.runBaseline(out)
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
