package sparkwrapper

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import symbolicprimitives.Utils
import trackers.{BaseTracker, RoaringBitmapTracker, Trackers}

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(sc: SparkContext) {
  def textFile(filepath: String): WrappedRDD[String] = {
    val rdd = sc.textFile(filepath)
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .map({case (value, id) => Trackers.createTracker(value, id)})
    new WrappedRDD[String](tracked_rdd)
  }

}