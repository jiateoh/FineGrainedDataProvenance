package examples.benchmarks.bigsift

import java.util.Calendar
import java.util.logging.{FileHandler, Level, LogManager, Logger}

import org.apache.spark.SparkContext
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.rdd.Lineage
import org.apache.spark.lineage.ui.{BSListenerBusImpl, FaultLocalizationInfo}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class BigSift(sc:SparkContext, logFile : String) {
  var bus: BSListenerBusImpl = null
  var local = 0
  val lm: LogManager = LogManager.getLogManager
  val fh: FileHandler = new FileHandler("myLog")
  val lc = new LineageContext(sc)
  lc.setCaptureLineage(true);
  val input = lc.textFile(logFile)



 // def runWithBigSift[T](f : (RDD[String] , Lineage[String]) => RDD[T] , test : Option[T => Boolean] ) : Unit = {
   // runWithBigSift(input , f, test)
  //}


    /**
    * Input RDD initiated with Lineage Context
    * */
def runWithBigSift[T](f : (RDD[String] , Lineage[String]) => RDD[T] , test : Option[T => Boolean] ) : Unit = {
  val logger: Logger = Logger.getLogger(getClass.getName)
  logger.setLevel(Level.INFO)
  def getFaultOutputTags( out : Array[(T,Long)]  ): Any => Boolean = {

    var testtype: Int = bus.testType
    // var udt : Any=>Boolean = bus.test.get
    var e = out.map(_._1).head

    if (e.isInstanceOf[Tuple2[_, _]]) {
      e.asInstanceOf[Tuple2[Any, Any]]._2 match {
        case a: Int =>
          var ar = out.asInstanceOf[Array[((Any, Int), Long)]]
          var ordr = new Ordering[((Any, Int), Long)] {
            def compare(x: ((Any, Int), Long), y: ((Any, Int), Long)): Int = {
              x._1._2 compare y._1._2
            }
          }
          testtype match {
            case 1 =>
              (s =>
                s.asInstanceOf[(Any, Int)]._2 <= ar.min(ordr)._1._2
                )
            case 2 =>
              (s =>
                s.asInstanceOf[(Any, Int)]._2 >= ar.max(ordr)._1._2
                )
            case 3 =>
              (s => s == null)
            case 4 =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
            case 5 =>
              bus.test.get
            case _ =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
          }
        case a: Float =>
          var ar = out.asInstanceOf[Array[((Any, Float), Long)]]
          var ordr = new Ordering[((Any, Float), Long)] {
            def compare(x: ((Any, Float), Long), y: ((Any, Float), Long)): Int = {
              x._1._2 compare y._1._2
            }
          }
          testtype match {
            case 1 =>
              (s =>
                s.asInstanceOf[(Any, Float)]._2 <= ar.min(ordr)._1._2
                )
            case 2 =>
              (s =>
                s.asInstanceOf[(Any, Float)]._2 >= ar.max(ordr)._1._2
                )
            case 3 =>
              (s => s == null)
            case 4 =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
            case 5 =>
              bus.test.get
            case _ =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
          }

      }

    } else {
      e match {
        case a: Int =>
          var ar = out.asInstanceOf[Array[(Int, Long)]]
          var ordr = new Ordering[(Int, Long)] {
            def compare(x: (Int, Long), y: (Int, Long)): Int = {
              x._1 compare y._1
            }
          }
          testtype match {
            case 1 =>
              (s =>
                s.asInstanceOf[Int] <= ar.min(ordr)._1
                )
            case 2 =>
              (s =>
                s.asInstanceOf[Int] >= ar.max(ordr)._1
                )
            case 3 =>
              (s => s == null)
            case 4 =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
            case 5 =>
              bus.test.get
            case _ =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
          }
        case a: Float =>
          var ar = out.asInstanceOf[Array[(Float, Long)]]
          var ordr = new Ordering[(Float, Long)] {
            def compare(x: (Float, Long), y: (Float, Long)): Int = {
              x._1 compare y._1
            }
          }
          testtype match {
            case 1 =>
              (s =>
                s.asInstanceOf[Float] <= ar.min(ordr)._1
                )
            case 2 =>
              (s =>
                s.asInstanceOf[Float] >= ar.max(ordr)._1
                )
            case 3 =>
              (s => s == null)
            case 4 =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
            case 5 =>
              bus.test.get
            case _ =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
          }

        case _ =>
          println("Type Not Supported")
          (s => true)
      }
    }
  }
  def getFaultOutputTagsDD( out : mutable.ArraySeq[(T)]): Any => Boolean ={

    var testtype:Int = bus.testType
//    var udt : Any=>Boolean = bus.test.get
    var e = out.head

    if (e.isInstanceOf[Tuple2[_, _]]) {
      e.asInstanceOf[Tuple2[Any, Any]]._2 match {
        case a: Int =>
          var ar = out.asInstanceOf[mutable.ArraySeq[(Any, Int)]]
          var ordr = new Ordering[(Any, Int)] {
            def compare(x: (Any, Int), y: (Any, Int)): Int = {
              x._2 compare y._2
            }
          }
          testtype match {
            case 1 =>
              (s =>
                s.asInstanceOf[(Any, Int)]._2 <= ar.min(ordr)._2
                )
            case 2 =>
              (s =>
                s.asInstanceOf[(Any, Int)]._2 >= ar.max(ordr)._2
                )
            case 3 =>
              (s => s == null)
            case 4 =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
            case 5 =>
              bus.test.get
            case _ =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
          }
        case a:  Float =>
          var ar = out.asInstanceOf[mutable.ArraySeq[(Any, Float)]]
          var ordr = new Ordering[(Any, Float)] {
            def compare(x: (Any, Float), y: (Any, Float)): Int = {
              x._2 compare y._2
            }
          }
          testtype match {
            case 1 =>
              (s =>
                s.asInstanceOf[(Any, Float)]._2 <= ar.min(ordr)._2
                )
            case 2 =>
              (s =>
                s.asInstanceOf[(Any, Float)]._2 >= ar.max(ordr)._2
                )
            case 3 =>
              (s => s == null)
            case 4 =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
            case 5 =>
              bus.test.get
            case _ =>
              logger.log(Level.INFO, "Unsupported test type")
              (s => true)
          }
      }
      }else{
        e match {
      case a : Int =>
        var ar = out.asInstanceOf[mutable.ArraySeq[(Int)]]
        var ordr = new Ordering[(Int)] {
          def compare(x:(Int),y:(Int)): Int = {
            x compare y
          }
        }
        testtype match {
          case 1 =>
            (s =>
              s.asInstanceOf[Int] <= ar.min(ordr)
              )
          case 2 =>
            (s =>
              s.asInstanceOf[Int] >= ar.max(ordr)
              )
          case 3 =>
            (s => s == null)
          case 4 =>
            logger.log(Level.INFO, "Unsupported test type")
            (s => true)
          case 5 =>
            bus.test.get
          case _ =>
            logger.log(Level.INFO, "Unsupported test type")
            (s => true)
        }
      case a : Float =>
        var ar = out.asInstanceOf[mutable.ArraySeq[(Float )]]
        var ordr = new Ordering[(Float)] {
          def compare(x:(Float),y:(Float)): Int = {
            x compare y
          }
        }
        testtype match {
          case 1 =>
            (s =>
              s.asInstanceOf[Float] <= ar.min(ordr)
              )
          case 2 =>
            (s =>
              s.asInstanceOf[Float] >= ar.max(ordr)
              )
          case 3 =>
            (s => s == null)
          case 4 =>
            logger.log(Level.INFO, "Unsupported test type")
            (s => true)
          case 5 =>
            bus.test.get
          case _ =>
            logger.log(Level.INFO, "Unsupported test type")
            (s => true)
        }

      case _ =>
        println("Type Not Supported")
        (s => true)
    }
    }
  }

  bus = new BSListenerBusImpl( lc.sparkContext.getConf, Some(lc.sparkContext))
  bus.jobId = 1
  val s_init = input.asInstanceOf[Lineage[String]].count()
  bus.postInitialSize(s_init)

  /**************************
        Time Logging
    **************************/
  val jobStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  val jobStartTime = System.nanoTime()
  logger.log(Level.INFO, "JOb starts at " + jobStartTimestamp)
  /**************************
        Time Logging
    **************************/


  val output =   f(null, input.asInstanceOf[Lineage[String]]).asInstanceOf[Lineage[T]]
    val out = output.collectWithId()
  /** ************************
        Time Logging
    * *************************/
  println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
  val jobEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  val jobEndTime = System.nanoTime()
  logger.log(Level.INFO, "JOb ends at " + jobEndTimestamp)
  logger.log(Level.INFO, "JOb span at " + (jobEndTime - jobStartTime) / 1000 + "milliseconds")
  bus.postInitialJobTime((jobEndTime -jobStartTime)/1000)
  /** ************************
        Time Logging
    * *************************/

  if(out.size > 1000){
    bus.postOutput(out.take(1000).map(s => s._1).map(_.toString()).reduce(_+"~"+_) ,out.take(1000).map(s => s._1) )
  }else bus.postOutput(out.map(s => s._1).map(_.toString()).reduce(_+"~"+_) ,out.map(s => s._1) )

  //stop capturing lineage information
  lc.setCaptureLineage(false)
  Thread.sleep(1000)


  var testimpl : T => Boolean = null;
  var ddtest : T => Boolean =null;
  if(test.isDefined){
    //bus.waitForUICommand();
    testimpl = test.get
    ddtest = test.get
  }else{
    bus.waitForUICommand();
    testimpl =  getFaultOutputTags(out)
    ddtest =  getFaultOutputTagsDD(out.map(_._1))
  }


  //list of bad inputs
  var list = List[Long]()

  for (o <- out) {
    if(testimpl(o._1))
      list = o._2 :: list
  }

  /** ************************
        Time Logging
    * *************************/
  val lineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  val lineageStartTime = System.nanoTime()
  logger.log(Level.INFO, "JOb starts at " + lineageStartTimestamp)
  /** ************************
        Time Logging
    * *************************/


  var linRdd = output.getLineage()
  linRdd.collect
  var temp = linRdd;

  linRdd = linRdd.filter { l => list.contains(l)}
  //println(linRdd.id + "  "+  linRdd.count())
  //println(temp.count())
  //println(linRdd.goBack().count())
  var map1 = mutable.Map[Int, (RDD[Any] , Long)]()
  linRdd = linRdd.goBackAll(map = map1)
  //println(">>>>>>>" + map1)
 // for((k,v) <- map1){
  //  println(">>>>>>>" + k + "  " + v._1.count() + "  -> " + v._2)
 // }
  bus.setDAGInfoMap(map1)

  val mappedRDD = linRdd.show(false).toRDD

  /**************************
        Time Logging
    **************************/
  println(">>>>>>>>>>>>>  First Job Done  <<<<<<<<<<<<<<<")
  val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  val lineageEndTime = System.nanoTime()
  logger.log(Level.INFO, "JOb ends at " + lineageEndTimestamp)
  logger.log(Level.INFO, "JOb span at " + (lineageEndTime-lineageStartTime)/1000 + "milliseconds")
  titian_time = (lineageEndTime-lineageStartTime)/1000
  titan_size = mappedRDD.count()
  /**************************
        Time Logging
    **************************/


  /**************************
        Time Logging
    **************************/
  val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  val DeltaDebuggingStartTime = System.nanoTime()
  logger.log(Level.INFO, "Record DeltaDebugging + L  (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)
  /**************************
        Time Logging
    **************************/

  bus.postToAll(FaultLocalizationInfo(0,s_init,0,Array()))
  val delta_debug = new DDNonExhaustive[String]
  delta_debug.bus = bus
  delta_debug.setMoveToLocalThreshold(local)
  val testdd = new TestDD[T](f,ddtest)
  val returnedRDD = delta_debug.ddgen(mappedRDD, testdd, new SequentialSplit[String], lm, fh , DeltaDebuggingStartTime)
  /**************************
        Time Logging
    **************************/
  val DeltaDebuggingEndTime = System.nanoTime()
  val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
  logger.log(Level.INFO, "DeltaDebugging (unadjusted) + L  ends at " + DeltaDebuggingEndTimestamp)
  logger.log(Level.INFO, "DeltaDebugging (unadjusted)  + L takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")
  bus.postFinalLocalizationTime((DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 )

  /**************************
        Time Logging
    **************************/
 // lc.sparkContext.stop()

}

  var titan_size = 0L
  var titian_time = 0L

  def getDebuggingStatistics(): Unit ={
    println(
      s"""
         | Total BigSift Debugging Time : ${bus.totalLocalizationTime.getOrElse(-1)} microseconds
         | Total Input Rows             : ${bus.initialSize.getOrElse(-1)} records
         | Total Titan Trace Size       : ${titan_size} records
         | Total Titian Time            : ${titian_time} microseconds
         |""".stripMargin)

  }

}
