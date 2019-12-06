/**
  * Created by malig on 12/3/19.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.Utils



object  WCDPI {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sdp = new SparkContextWithDP(sc)
    val input = sdp.textFile("data/xah")
    val count = input
      .flatMap(s => s.split(' ')).map(s => (s,1))
      .reduceByKey(_ + _)
     .collect()

   // count.foreach(println)
      //
      //.reduceByKey(_ + _)//.filter(s => s._1.getValue().contains("ali"))
      //.collect()
    println(count.head)
    Utils.retrieveProvenance(count.head.bitmap)
//    count.foreach(println)
    // Measuring Storage overhead
//    println(count.map(a => a.bitmap.getSizeInBytes).reduce(_+_) + " Bytes")
    //count.map(a => a._2.getProvenanceSize()).reduce(_+_) +
  //  println(count.head)
    // Getting Provenance here
    //Utils.retrieveProvenance(count.head.bitmap)
  }
}


object  WC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val input = sc.textFile("data/xah")
    val count = input.flatMap(s => s.split(' '))
      .map(s => (s,1))
      .reduceByKey(_ + _).filter(s => s._1.equals("piraso"))
      .collect()

        count.foreach(println)
    // Measuring Storage overhead
    //    println(count.map(a => a.bitmap.getSizeInBytes).reduce(_+_) + " Bytes")
    //count.map(a => a._2.getProvenanceSize()).reduce(_+_) +
    //  println(count.head)

    // Getting Provenance here
    //Utils.retrieveProvenance(count.head.bitmap)
  }
}
