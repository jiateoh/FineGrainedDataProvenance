package examples.benchmarks.bigsift

import java.util.logging.{FileHandler, LogManager}

import examples.benchmarks.bigsift.interfaces.Testing
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.rdd.RDD

class TestDD[T](f : (RDD[String] , Lineage[String]) => RDD[T] , test: T=>Boolean) extends Testing[String] with Serializable {
  var num = 0;
  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {    num = num + 1
    println( s""" >>>>>>>>>>>>>>>>>>>>>>>>>> The number of runs are $num <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<,""")
    var returnValue = false;
    val out = f(inputRDD ,null).collect().filter(test)
    for (o <- out) {
      returnValue = true
    }
    return returnValue
  }
  /**
    *
    * Does not support local for now.
    *
    */
  def usrTest(inputRDD: Array[String], lm: LogManager, fh: FileHandler): Boolean = {
    false
  }
}