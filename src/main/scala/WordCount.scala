import org.apache.spark.{SparkConf, SparkContext}
import symbolicprimitives.{SymInt, SymString, Utils}

/**
  * Created by malig on 10/31/19.
  */
object WordCount {

//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val input = sc.textFile("file.log")
//
//    val input_new = Utils.setInputZip(input.zipWithUniqueId()) // << Rewrites inserted
//      .map(s => new SymString(s._1, s._2)) // << Rewrites Inserted
//
//    // s x s
//    val count = input_new.flatMap(s => s.split(' '))
//      .map(s => (s, new SymInt(1, s.getProvenance())))
//      .reduceByKey(_ + _)//.filter(s => s._1.getValue().contains("ali"))
//      .collect()
//
//    // Measuring Storage overhead
//    println(count.map(a => a._2.getProvenanceSize()).reduce(_+_) + " Bytes")
//    //count.map(a => a._2.getProvenanceSize()).reduce(_+_) +
//    println(count.head)
//
//    // Getting Provenance here
//    Utils.retrieveProvenance(count.head._2.getProvenance())
  //}
}
