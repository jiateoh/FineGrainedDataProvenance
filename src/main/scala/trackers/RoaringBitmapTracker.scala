package trackers

import org.roaringbitmap.RoaringBitmap

import scala.reflect.ClassTag


/**
  * Created by malig on 12/3/19.
  * Ported to BaseTracker API by jteoh on 1/7/2020
  */

class RoaringBitmapTracker[T: ClassTag](value: T, var rr: RoaringBitmap) extends BaseTracker[T](value) {
  
  override def toString: String = s""" ${value.toString} --> ${rr.toString} Size: ${rr.getSizeInBytes} Bytes """
  
  
  override def withValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] = {
    new RoaringBitmapTracker(newValue, this.rr)
  }
  
  override def cloneWithValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] = {
    //new RoaringBitmapTracker[U](newValue, this.rr.clone())
    withValue(newValue)
  }
  
  override def mergeTrackerProvenance(other: BaseTracker[_]): BaseTracker[T] = {
    other match {
      case rbt: RoaringBitmapTracker[_] =>
        this.rr.or(rbt.rr)
        rbt.rr.clear()
        this
      case _ => throw new NotImplementedError(s"Unknown tracker type! $other")
    }
  }
  
  override def provenanceCount: Int = rr.getCardinality
  
  override def provenanceSizeEst: Long = rr.getSizeInBytes
}

