/**
  * Created by malig on 10/31/19.
  */

import org.apache.spark._
import symbolicprimitives.SymString
import symbolicprimitives.Utils

object StdDev {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//
//    val input = sc.textFile("file.log")
//
//     val input_new =  Utils.setInputZip(input.zipWithIndex()) // << Rewrites inserted
//      .map(s => new SymString(s._1, s._2)) // << Rewrites Inserted
//    // s x s
//    val sum_of_squares = input_new.map(s => s.toInt).map( a => a * a ).reduce(_+_)
//    val sum = input.map(s => s.toInt).reduce(_+_)
//    val square_of_sum = sum * sum
//    val count =  input.count()
//    val std = (sum_of_squares/count) - (square_of_sum/(count*count))
//    println(std)
//    Utils.retrieveProvenance(std.getProvenance())
  }

}
