package sparkwrapper

import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.roaringbitmap.RoaringBitmap
import sparkwrapper.WrappedRDD._
import symbolicprimitives.Tracker

import scala.collection.{Map, mutable}
import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */
class WrappedPairRDD[K, V](val rdd: RDD[(K, Tracker[V])])(
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
  
  // TODO: implement other PairRDD functions on top of combineByKeyWithClassTag
  /**
   * Optimized combineByKey implementation where provenance tracking data structures are
   * constructed only once per partition+key.
   */
  def combineByKeyWithClassTag[C](
                         createCombiner: V => C,
                         mergeValue: (C, V) => C,
                         mergeCombiners: (C, C) => C,
                         partitioner: Partitioner = defaultPartitioner(rdd),
                         mapSideCombine: Boolean = true,
                         serializer: Serializer = null)(implicit ct: ClassTag[C]): WrappedRDD[(K, C)] = {
  
    /** Updates combiner's bitmap in place and returns the updated instance */
    def updateCombinerBitmap(combiner: Tracker[_], rr2: RoaringBitmap): Unit = {
      // TODO: apply rank function here
      combiner.bitmap.or(rr2)
      // second bitmap can be pre-emptively cleared to ease memory pressure since this is
      // combineByKey.
      rr2.clear()
    }
  
    new WrappedRDD[(K, C)](
      rdd.combineByKeyWithClassTag[Tracker[C]](
        // init: create a new 'tracker' instance that we can reuse for all values in the key.
        (tracker: Tracker[V]) =>
            new Tracker(createCombiner(tracker.value), tracker.bitmap.clone()),
        (combiner: Tracker[C], next: Tracker[V]) => {
          combiner.payload = mergeValue(combiner.value, next.value)
          updateCombinerBitmap(combiner, next.bitmap)
          combiner
        },
        (combiner1: Tracker[C], combiner2: Tracker[C]) => {
          combiner1.payload = mergeCombiners(combiner1.value, combiner2.value)
          updateCombinerBitmap(combiner1, combiner2.bitmap)
          combiner1
        },
        partitioner,
        mapSideCombine,
        serializer
      ))
  }

  // START: Additional Spark-supported reduceByKey APIs
  def reduceByKey(func: (V, V) => V): WrappedRDD[(K, V)] = {
    combineByKeyWithClassTag(identity, func, func)
  }
  
  
  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
   */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): WrappedRDD[(K, V)] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }
  
  /**
   * Merge the values for each key using an associative and commutative reduce function. This will
   * also perform the merging locally on each mapper before sending results to a reducer, similarly
   * to a "combiner" in MapReduce.
   */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): WrappedRDD[(K, V)] =  {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }
  
  // END: Additional Spark-supported reduceByKey APIs
  
  
  def groupByKey(partitioner: Partitioner): WrappedRDD[(K, Iterable[V])] =  {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs: WrappedRDD[(K, CompactBuffer[V])] = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    
    // Final cast of CompactBuffer -> Iterable for API matching
    bufs.asInstanceOf[WrappedRDD[(K, Iterable[V])]]
  }
  
  def groupByKey(numPartitions: Int): WrappedRDD[(K, Iterable[V])] = {
    groupByKey(new HashPartitioner(numPartitions))
  }
  
  def groupByKey(): WrappedRDD[(K, Iterable[V])] = {
    groupByKey(defaultPartitioner(rdd))
  }
  /*** END NEW IMPLEMENTATION ***/
  
  def mapValues[U: ClassTag](f: V => U): WrappedRDD[(K, U)] = {
    new WrappedRDD[(K,U)](
      rdd.mapValues(
        v => new Tracker(f(v.value), v.bitmap)
      )
    )
  }
  
  def flatMapValues[U: ClassTag](f: V => TraversableOnce[U]): WrappedRDD[(K, U)] = {
    new WrappedRDD[(K,U)](
      rdd.flatMapValues(
        v => f(v.value).map(new Tracker(_, v.bitmap))
      )
    )
  }
  
  def values: WrappedRDD[V] = rdd.map(_._2)
  
  
  /** Join two RDDs while maintaining the key-key lineage. This operation is currently only
   * supported for RDDs that possess the same base input RDD.
   */
  def join[W](other: WrappedPairRDD[K, W],
              partitioner: Partitioner = defaultPartitioner(rdd)
             ): WrappedPairRDD[K, (V, W)] = {
    assert(rdd.firstSource == other.rdd.firstSource,
           "Provenance-based join is only supported for RDDs originating from the same input data" +
             " (e.g. self-join): " + s"${rdd.firstSource} vs. ${other.rdd.firstSource}")
    val result: RDD[(K, Tracker[(V, W)])] = rdd.cogroup(other.rdd).flatMapValues(pair =>
     for (thisTracker <- pair._1.iterator; otherTracker <- pair._2.iterator)
       yield new Tracker[(V, W)](
         // Value tuple
         (thisTracker.value, otherTracker.value),
         // TODO: extend this implementation to support case when joining two different datasets.
         RoaringBitmap.or(
           thisTracker.bitmap,
           otherTracker.bitmap
           )
         )
      // warning: can't clear these two bitmaps as they may be reused!

                                                                                 )
    new WrappedPairRDD(result)
  }
  
  def collectAsMapWithTrackers(): Map[K, Tracker[V]] = {
    val data = rdd.collect()
    val map = new mutable.HashMap[K, Tracker[V]]
    map.sizeHint(data.length)
    data.foreach { pair => map.put(pair._1, pair._2) }
    map
  }
  
  def collectAsMap(): Map[K, V] = {
    collectAsMapWithTrackers().mapValues(_.value)
  }
  
  def setName(name: String): this.type = {
    rdd.setName(name)
    this
  }
  
  implicit class RDDWithDataSource(rdd: RDD[_]) {
    def firstSource: RDD[_] = {
      rdd.allSources.head
    }
    
    def allSources: Seq[RDD[_]] = {
      if(rdd.dependencies.isEmpty) {
        Seq(rdd)
      } else {
        rdd.dependencies.map(_.rdd).flatMap(_.allSources).distinct
      }
    }
  }
  
  
}
