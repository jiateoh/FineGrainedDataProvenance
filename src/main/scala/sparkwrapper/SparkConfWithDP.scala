package sparkwrapper

import org.apache.spark.SparkConf
import org.roaringbitmap.RoaringBitmap
import provenance.{DummyProvenance, Provenance, RoaringBitmapProvenance}
import trackers.{BaseTracker, RoaringBitmapTracker, SetTracker, Trackers}

import scala.collection.mutable

class SparkConfWithDP(withKryo: Boolean = true, withTrackers: Boolean = false) extends SparkConf {
  if(withKryo) {
    set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if(withTrackers) {
      registerKryoClasses(Array(classOf[RoaringBitmapTracker[Int]], classOf[RoaringBitmapTracker[Long]],
                                classOf[RoaringBitmapTracker[Short]], classOf[RoaringBitmapTracker[Boolean]],
                                classOf[RoaringBitmapTracker[Double]], classOf[RoaringBitmapTracker[String]],
                                classOf[RoaringBitmap],
                                classOf[SetTracker[Int]], classOf[SetTracker[Long]],
                                classOf[SetTracker[Short]], classOf[SetTracker[Boolean]],
                                classOf[SetTracker[Double]], classOf[SetTracker[String]],
                                classOf[mutable.Set[Int]],
                                classOf[BaseTracker[Int]], classOf[BaseTracker[Long]],
                                classOf[BaseTracker[Short]], classOf[BaseTracker[Boolean]],
                                classOf[BaseTracker[Double]], classOf[BaseTracker[String]]))
    } else {
      registerKryoClasses(Array(classOf[RoaringBitmapProvenance], classOf[DummyProvenance],
                                classOf[RoaringBitmap]))
    }
    
      
  }
}
