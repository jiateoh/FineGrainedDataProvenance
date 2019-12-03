package sparkwrapper

import org.apache.spark.SparkContext
import org.roaringbitmap.RoaringBitmap
import symbolicprimitives.{AnyTracker, Utils}

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(sc : SparkContext) {
  def textFile(filepath: String): WrappedRDD[String] ={
    val rdd = sc.textFile(filepath)
    val tracked_rdd = Utils.setInputZip(rdd.zipWithIndex())
      .map{s =>
        val rr = new RoaringBitmap
        if(s._2 > Int.MaxValue) throw new UnsupportedOperationException("The offset is greater than Int.Max which is not supported yet")
        rr.add(s._2.asInstanceOf[Int])
        new AnyTracker(s._1, rr)
      }
    return new WrappedRDD[String](tracked_rdd)
  }


}
