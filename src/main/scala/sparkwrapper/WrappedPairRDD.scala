package sparkwrapper

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.roaringbitmap.RoaringBitmap
import sparkwrapper.WrappedRDD._
import symbolicprimitives.Tracker
import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */
class WrappedPairRDD[K, V](rdd: RDD[(K, Tracker[V])])(
    implicit kt: ClassTag[K],
    vt: ClassTag[V],
    ord: Ordering[K] = null)
    extends Serializable {
  
  /*** START OLD IMPLEMENTATION ***/
  // Unused/safe implementation
  def rankBitmapsOriginal(rr1: RoaringBitmap, rr2: RoaringBitmap): RoaringBitmap = {
    RoaringBitmap.or(rr1, rr2)
 //   rr1
  }
  
  def reduceByKeyOriginal(func: (V, V) => V): WrappedRDD[(K, V)] =
    return new WrappedRDD[(K, V)](rdd.reduceByKey { (v1, v2) =>
      val value = func(v1.value, v2.value)
      new Tracker(value, rankBitmapsOriginal(v1.bitmap, v2.bitmap))
    })
  
  /*** END OLD IMPLEMENTATION ***/
  
  
  /*** START NEW IMPLEMENTATION ***/
  /** Updates combiner's bitmap in place and returns the updated instance */
  def updateCombinerBitmap(combiner: TrackerCombiner[_], rr2: RoaringBitmap): RoaringBitmap = {
    // TODO: apply rank function here
    combiner.bitmap.or(rr2)
    combiner.bitmap
  }
  
  private class TrackerCombiner[C](var value: C, var bitmap: RoaringBitmap)
                                  extends Serializable {
  }
  
  // TODO: implement other PairRDD functions on top of combineByKeyWithClassTag
  /**
   * Optimized combineByKey implementation where provenance tracking data structures are
   * constructed only once per partition+key.
   */
  def combineByKeyWithClassTag[C](
                         createCombiner: V => C,
                         mergeValue: (C, V) => C,
                         mergeCombiners: (C, C) => C,
                         partitioner: Partitioner = Partitioner.defaultPartitioner(rdd),
                         mapSideCombine: Boolean = true,
                         serializer: Serializer = null)(implicit ct: ClassTag[C]): WrappedRDD[(K, C)] = {
    
    
    new WrappedRDD[(K, C)](
      rdd.combineByKeyWithClassTag[TrackerCombiner[C]](
        // init: create a new 'tracker' instance that we can reuse for all values in the key.
        (tracker: Tracker[V]) =>
            new TrackerCombiner(createCombiner(tracker.value), tracker.bitmap.clone()),
        (combiner: TrackerCombiner[C], next: Tracker[V]) => {
          combiner.value = mergeValue(combiner.value, next.value)
          updateCombinerBitmap(combiner, next.bitmap)
          combiner
        },
        (combiner1: TrackerCombiner[C], combiner2: TrackerCombiner[C]) => {
          combiner1.value = mergeCombiners(combiner1.value, combiner2.value)
          updateCombinerBitmap(combiner1, combiner2.bitmap)
          combiner1
        },
        partitioner,
        mapSideCombine,
        serializer
      ).mapValues(
        // finally, convert back to Tracker class.
        combiner => new Tracker(combiner.value, combiner.bitmap))
    )
  }

  def reduceByKey(func: (V, V) => V): WrappedRDD[(K, V)] = {
    combineByKeyWithClassTag(identity, func, func)
  }
  
  /*** END NEW IMPLEMENTATION ***/
}
