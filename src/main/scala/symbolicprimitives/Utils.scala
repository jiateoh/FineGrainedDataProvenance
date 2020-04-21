package symbolicprimitives

import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import provenance.data.{DummyProvenance, InfluenceMarker, Provenance}
import provenance.data.InfluenceMarker.InfluenceMarker
import provenance.rdd.{ CombinerWithInfluence, ProvenanceRow}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.Combiner
import scala.reflect.ClassTag

/**
  * Created by malig on 5/3/19.
  */
object PackIntIntoLong {
  private final val RIGHT: Long = 0xFFFFFFFFL

  def apply(left: Int, right: Int): Long =
    left.toLong << 32 | right & 0xFFFFFFFFL

  def getLeft(value: Long): Int =
    (value >>> 32).toInt // >>> operator 0-fills from left

  def getRight(value: Long): Int = (value & RIGHT).toInt
}

object Utils {

  def print(s: String): Unit = {
    println("\n" + s)
  }

  private var zipped_input_RDD: RDD[(String, Long)] = null;
  def setInputZip(rdd: RDD[(String, Long)]): RDD[(String, Long)] = {
    zipped_input_RDD = rdd
    rdd
  }

  def retrieveProvenance(rr: RoaringBitmap): RDD[String] = {
    val rdd = zipped_input_RDD
      .filter(s => rr.contains(s._2.asInstanceOf[Int]))
      .map(s => s._1)
    rdd.collect().foreach(println)
    rdd
  }

  /** Utility method to extract symbolic provenance from object to construct provenance row, if
    * applicable. This method should not be used if udfAware is disabled! */
  private def buildSymbolicProvenanceRow[T](in: T, rowProv: Provenance): ProvenanceRow[T] = {
    // Might be worth looking into classtags to see if we can avoid this runtime check altogether
    // and simply define methods beforehand.
    in match {
      case o: SymBase =>
        (in, o.getProvenance())
      case _ =>
        (in, rowProv)
    }
  }
  
  def computeOneToOneUDF[T, U](f: T => U,
                               input: ProvenanceRow[T],
                               udfAware: Boolean): ProvenanceRow[U] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    if(udfAware) {
      buildSymbolicProvenanceRow(f(input._1), input._2)
    } else {
      input._1 match {
        case r: SymBase =>
          // Note: this is an optimization that must be done *before* calling f on the input.
          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
        case _ =>
      }
      (f(input._1), input._2) // use input provenance and pass it to next operator
    }
//    input._1 match {
//      case r: SymBase =>
//        if (!udfAware) {
//          // Note: this is an optimization that must be done *before* calling f on the input.
//          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
//          (out, input._2) // use input provenance and pass it to next operator
//        } else {
//          buildSymbolicProvenanceRow(out, input._2)
//        }
//      case r => {
//        if(!udfAware)
//          (out, input._2)
//        else {
//          buildSymbolicProvenanceRow(out, input._2)
//        }
//      }
//    }
  }

  // TODO: Consider utilizing ProvenanceGrouping object??
  def computeOneToManyUDF[T, U](
      f: T => TraversableOnce[U],
      input: ProvenanceRow[T],
      udfAware: Boolean): TraversableOnce[ProvenanceRow[U]] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    if(udfAware) {
      f(input._1).map(buildSymbolicProvenanceRow(_, input._2))
    } else {
      input._1 match {
        case r: SymBase =>
          // Note: this is an optimization that must be done *before* calling f on the input.
          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
        case _ =>
      }
      f(input._1).map((_, input._2)) // use input provenance and pass it to next operator
    }
    
//    input._1 match {
//      case r: SymBase =>
//        if (!udfAware) {
//          r.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
//          f(input._1).map((_, input._2))
//        } else {
//          f(input._1).map { out =>
//            out match {
//              case o: SymBase =>
//                (out, o.getProvenance())
//              case a => (a, input._2)
//            }
//          }
//        }
//      case r =>
//        f(r).map((_, input._2))
//    }
  }

  /** Takes in two provenance rows and returns the one that is selected by the Marker.
    * V here is the source value which has the most inlfuence on the combinerd output
    * It is not the combined output [Gulzar] */
  private def mergeWithInfluence[V](prov: ProvenanceRow[V],
                                 prov_other: ProvenanceRow[V],
                                 infl: InfluenceMarker): ProvenanceRow[V] = {
    infl match {
      case InfluenceMarker.right => prov_other
      case InfluenceMarker.left  => prov
      case InfluenceMarker.both  => (prov._1 , prov._2.merge(prov_other._2))
    }
  }


  /**
  * Combined combiner with a value and rank the provenance based on the influence function if given
  * Also check if UDFAwareProvenance is enabled
  */
  def computeCombinerWithValueUDF[C, V](f: (C, V) => C,
                               combiner: ProvenanceRow[CombinerWithInfluence[C,V]],
                               value: ProvenanceRow[V],
                               udfAware: Boolean,
                               inflFunction: Option[(V, V) => InfluenceMarker] =
                                 None): ProvenanceRow[CombinerWithInfluence[C,V]] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    // TODO: optimization on provenance - if the output of f (ie, the combiner) is symbolic, we
    //  can extract its provenance assuming udfAware is true. This is independent of input types.
    // TODO: both input arguments can individually have their provenance disabled if necessary,
    //  rather than simultaneously (e.g. if only one of V/C are symbolic, disable accordingly).
    //  (this is only a performance improvement).
    (value._1, combiner._1._1) match {
      case (v: SymBase, c: SymBase) =>
        if (!udfAware) {

          v.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          c.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          val combiner_influence  = (combiner._1._2 , combiner._2)
          val  (infl_value, prov) =  mergeWithInfluence(combiner_influence,value , inflFunction.getOrElse((c: V, v: V) =>
              InfluenceMarker.both)(combiner._1._2, value._1))
          ( (f(combiner._1._1, value._1), infl_value ) ,prov )

        } else {

          val out = f(combiner._1._1, value._1)
          out match {
            case o: SymBase =>
              ( ( out, value._1),  o.getProvenance())
            case a =>
              val combiner_influence  = (combiner._1._2 , combiner._2)
              val  (infl_value, prov) =  mergeWithInfluence(combiner_influence,value , InfluenceMarker.both)
              ( (a, infl_value ) ,prov )
          }
        }

      case r =>
        val combiner_influence  = (combiner._1._2 , combiner._2)
        val  (infl_value, prov) =  mergeWithInfluence(combiner_influence,value , inflFunction.getOrElse((c: V, v: V) =>
          InfluenceMarker.both)(combiner._1._2, value._1))
        ( (f(combiner._1._1, value._1), infl_value ) , if(udfAware) DummyProvenance.create() else prov )
    }
  }
  

  /**
    * Combined combiner with another combiner and rank the provenance based on the influence function if given
    * otherwise merges the provenance.
    *
    * Also check if UDFAwareProvenance is enabled
    * */
  def computeCombinerWithCombinerUDF[C, V](f: (C, C) => C,
                                        combiner1: ProvenanceRow[CombinerWithInfluence[C,V]],
                                        combiner2: ProvenanceRow[CombinerWithInfluence[C,V]],
                                        udfAware: Boolean,
                                        inflFunction: Option[(V, V) => InfluenceMarker] =
                                        None): ProvenanceRow[CombinerWithInfluence[C,V]] = {
    // Note: udfAware should be determined by now, as app-level wide configurations are not
    // properly persisted in a distributed setting. In other words, this method is used at
    // runtime rather than DAG building time, and the parameter should have been already
    // determined earlier.
    // TODO: optimization on provenance - if the output of f (ie, the combiner) is symbolic, we
    //  can extract its provenance assuming udfAware is true. This is independent of input types.
    // TODO: both input arguments can individually have their provenance disabled if necessary,
    //  rather than simultaneously (e.g. if only one of the two are symbolic, disable accordingly).
    //  (this is only a performance improvement).
    (combiner1._1._1 , combiner2._1._1) match {
      case (v: SymBase, c: SymBase) =>
        if (!udfAware) {

          v.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          c.setProvenance(DummyProvenance.create()) // Let sym objects use a dummy provenance
          val  (infl_value, prov) =  mergeWithInfluence((combiner1._1._2 , combiner1._2),(combiner2._1._2 , combiner2._2), inflFunction.getOrElse((c: V, v: V) =>
            InfluenceMarker.both)(combiner1._1._2, combiner2._1._2))
          ( (f(combiner1._1._1 , combiner2._1._1), infl_value ) ,prov )

        } else {

          val out = f(combiner1._1._1 , combiner2._1._1)
          out match {
            case o: SymBase =>
              ( ( out, combiner1._1._2),  o.getProvenance()) // Using a random influence V.
            case a =>
              val  (infl_value, prov) =  mergeWithInfluence((combiner1._1._2 , combiner1._2),(combiner2._1._2 , combiner2._2), InfluenceMarker.both)
              ( (a, infl_value ) ,prov )
          }
        }

      case r =>
        val  (infl_value, prov) =  mergeWithInfluence((combiner1._1._2 , combiner1._2),(combiner2._1._2 , combiner2._2) , inflFunction.getOrElse((c: V, v: V) =>
          InfluenceMarker.both)(combiner1._1._2, combiner2._1._2))
        ( (f(combiner1._1._1 , combiner2._1._1), infl_value ) ,if(udfAware) DummyProvenance.create() else prov  )
    }
  }

  def createCombinerForReduce[V,C](createCombiner: V => C , value: V, prov: Provenance, udfAware:Boolean):
  ProvenanceRow[CombinerWithInfluence[C,V]] = {
    ((createCombiner(value) , value), prov)
  }
  
  
  /** Default application configuration flag to determine whether or not to propogate row-level
    * provenance when symbolic objects are used. */
  private var defaultUDFAwareEnabled = false
  
  /** Application configuration flag to determine whether or not to propogate row-level
    * provenance when symbolic objects are used. Defaults to internal variable if None. */
  def getUDFAwareEnabledValue(opt: Option[Boolean]): Boolean = {
    opt.getOrElse(defaultUDFAwareEnabled)
  }
  
  def setUDFAwareDefaultValue(value: Boolean): Unit = {
    defaultUDFAwareEnabled = value
  }
  
  // A regular expression to match classes of the internal Spark API's
  // that we want to skip when finding the call site of a method.
  private val SPARK_CORE_CLASS_REGEX =
    """^org\.apache\.spark(\.api\.java)?(\.util)?(\.rdd)?(\.broadcast)?\.[A-Z]""".r
  private val SYMEx_CORE_REGEX = """^main\.symbolicprimitives.*""".r

  /** Default filtering function for finding call sites using `getCallSite`. */
  private def sparkInternalExclusionFunction(className: String): Boolean = {
    val SCALA_CORE_CLASS_PREFIX = "scala"
    val isSparkClass = SPARK_CORE_CLASS_REGEX
      .findFirstIn(className)
      .isDefined ||
      SYMEx_CORE_REGEX.findFirstIn(className).isDefined
    val isScalaClass = className.startsWith(SCALA_CORE_CLASS_PREFIX)
    // If the class is a Spark internal class or a Scala class, then exclude.
    isSparkClass || isScalaClass
  }

  def getCallSite(skipClass: String => Boolean = sparkInternalExclusionFunction)
    : CallSite = {
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
