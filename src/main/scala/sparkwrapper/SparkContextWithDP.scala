package sparkwrapper

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import symbolicprimitives.{Tracker, Utils}

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(sc: SparkContext) {
  def textFile(filepath: String): WrappedRDD[String] = {
    val rdd = sc.textFile(filepath)
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

}
