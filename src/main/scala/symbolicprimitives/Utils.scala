package symbolicprimitives

import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by malig on 5/3/19.
  */

object PackIntIntoLong {
  private final val RIGHT: Long = 0xFFFFFFFFL

  def apply(left: Int, right: Int): Long = left.toLong << 32 | right & 0xFFFFFFFFL

  def getLeft(value: Long): Int = (value >>> 32).toInt // >>> operator 0-fills from left

  def getRight(value: Long): Int = (value & RIGHT).toInt
}


object Utils {

  def print(s:String): Unit ={
    println("\n" + s)
  }

  private var zipped_input_RDD : RDD[(String,Long)] = null;
  def setInputZip(rdd: RDD[(String,Long)]): RDD[(String,Long)] ={
    zipped_input_RDD = rdd
    rdd
  }

  def retrieveProvenance(rr: RoaringBitmap): RDD[String] ={
    val rdd = zipped_input_RDD.filter(s => rr.contains(s._2.asInstanceOf[Int])).map(s => s._1)
    rdd.collect().foreach(println)
    rdd
  }

  // A regular expression to match classes of the internal Spark API's
  // that we want to skip when finding the call site of a method.
  private val SPARK_CORE_CLASS_REGEX =
  """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  private val SYMEx_CORE_REGEX = """^main\.symbolicprimitives.*""".r

  /** Default filtering function for finding call sites using `getCallSite`. */
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX.findFirstIn(className).isDefined ||
      SYMEx_CORE_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkClass || isScalaClass
  }

  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction): CallSite = {
    var lastSparkMethod = "<unknown>"
    var firstUserFile = "<unknown>"
    var firstUserLine = 0
    var insideSpark = true
    val callStack = new ArrayBuffer[String]() :+ "<unknown>"

    Thread.currentThread.getStackTrace().foreach { ste: StackTraceElement =>
      if (ste != null && ste.getMethodName != null
        && !ste.getMethodName.contains("getStackTrace")) {
        if (insideSpark) {
          if (skipClass(ste.getClassName)) {
            lastSparkMethod = if (ste.getMethodName == "<init>") {
              // Spark method is a constructor; get its class name
              ste.getClassName.substring(ste.getClassName.lastIndexOf('.') + 1)
            } else {
              ste.getMethodName
            }
            callStack(0) = ste.toString // Put last Spark method on top of the stack trace.
          } else {
            if (ste.getFileName != null) {
              firstUserFile = ste.getFileName
              if (ste.getLineNumber >= 0) {
                firstUserLine = ste.getLineNumber
              }
            }
            callStack += ste.toString
            insideSpark = false
          }
        } else {
          callStack += ste.toString
        }
      }
    }

    val callStackDepth = System.getProperty("spark.callstack.depth", "20").toInt
    val shortForm =
      if (firstUserFile == "HiveSessionImpl.java") {
        // To be more user friendly, show a nicer string for queries submitted from the JDBC
        // server.
        "Spark JDBC Server Query"
      } else {
       // s"$lastSparkMethod at $firstUserFile:$firstUserLine"
         s"$firstUserFile:$firstUserLine"

      }
    val longForm = callStack.take(callStackDepth).mkString("\n")

    CallSite(shortForm, longForm)
  }
}

/** CallSite represents a place in user code. It can have a short and a long form. */
case class CallSite(shortForm: String, longForm: String)
