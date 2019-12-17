package sparkwrapper

import org.apache.spark.{SparkConf, SparkContext}
import org.roaringbitmap.RoaringBitmap
import symbolicprimitives.{Tracker, Utils}

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(val sc: SparkContext) {
  def textFile(filepath: String,
               minPartitions: Int = sc.defaultMinPartitions): WrappedRDD[String] = {
    val rdd = sc.textFile(filepath, minPartitions)
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .map { s =>
        val rr = new RoaringBitmap
        if (s._2 > Int.MaxValue)
          throw new UnsupportedOperationException(
            "The offset is greater than Int.Max which is not supported yet")
        rr.add(s._2.asInstanceOf[Int])
        new Tracker(s._1, rr)
      }
    return new WrappedRDD[String](tracked_rdd)
  }
  
  def stop(): Unit = {
    sc.stop()
  }
  
  implicit def dpToSC(dp: SparkContextWithDP): SparkContext = dp.sc
}
