package sparkwrapper

import org.apache.spark.rdd.PairRDDFunctions
import symbolicprimitives.Tracker
import sparkwrapper.WrappedRDD._
import org.roaringbitmap.RoaringBitmap

import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */
class WrappedPairRDD[K, V](rdd: PairRDDFunctions[Tracker[K], Tracker[V]])(
    implicit kt: ClassTag[K],
    vt: ClassTag[V],
    ord: Ordering[K] = null)
    extends Serializable {
  def rankBitmaps(rr1: RoaringBitmap, rr2: RoaringBitmap): RoaringBitmap = {
    rr1.or(rr2)
    rr1
  }
  // TODO: apply rank function here
  def reduceByKey(func: (V, V) => V): WrappedRDD[(K, V)] =
    return new WrappedRDD[(K, V)](rdd.reduceByKey { (v1, v2) =>
      val value = func(v1.value, v2.value)
      rankBitmaps(v1.bitmap, v2.bitmap)
      new Tracker(value, v1.bitmap)
    })

}
