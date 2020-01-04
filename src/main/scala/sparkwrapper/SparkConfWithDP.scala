package sparkwrapper

import org.apache.spark.SparkConf
import org.roaringbitmap.RoaringBitmap
import symbolicprimitives.Tracker

class SparkConfWithDP(withKryo: Boolean = true) extends SparkConf {
  if(withKryo) {
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    registerKryoClasses(Array(classOf[Tracker[Int]], classOf[Tracker[Long]],
                              classOf[Tracker[Short]], classOf[Tracker[Boolean]],
                              classOf[Tracker[Double]], classOf[Tracker[String]],
                              classOf[RoaringBitmap]))
  }
}
