package sparkwrapper


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import symbolicprimitives.Tracker

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by malig on 12/3/19.
  */
class WrappedRDD[T: ClassTag](rdd: RDD[Tracker[T]]) extends Serializable {

  def getUnWrappedRDD(): RDD[Tracker[T]] = {
    return rdd
  }
  def map[U: ClassTag](f: T => U): WrappedRDD[U] = {
    return new WrappedRDD(rdd.map(s => new Tracker[U](f(s.value), s.bitmap)))
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): WrappedRDD[U] = {
    return new WrappedRDD(
      rdd.flatMap(a => f(a.value).map(s => new Tracker(s, a.bitmap))))
  }

  def filter(f: T => Boolean): WrappedRDD[T] = {
    return new WrappedRDD(rdd.filter(s => f(s.value)))
  }

  def collect(): Array[Tracker[T]] = {
    return rdd.collect()
  }
  
  def count():Long= {
    return rdd.count()
  }
  
  def take(num: Int): Array[Tracker[T]] = {
    rdd.take(num)
  }
  
  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): WrappedRDD[T] = {
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
  }
  
  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  def distinct(): WrappedRDD[T] = {
    distinct(rdd.partitions.length)
  }
  
  def persist(newLevel: StorageLevel): this.type = {
    rdd.persist(newLevel)
    this
  }
  
  def persist(): this.type = {
    rdd.persist()
    this
  }
  
  def unpersist(blocking: Boolean = true): this.type = {
    rdd.unpersist(blocking)
    this
  }
  
  def cache(): this.type = {
    rdd.cache()
    this
  }
  
  def takeSampleWithTracker(withReplacement: Boolean,
                            num: Int,
                            // should preferably use Spark's Utils.random
                            seed: Long = new Random().nextLong): Array[Tracker[T]] = {
    rdd.takeSample(withReplacement, num, seed)
  }
  
  def takeSample(withReplacement: Boolean,
                 num: Int,
                 // should technically  use Spark's Utils.random
                 seed: Long = new Random().nextLong): Array[T] = {
    takeSampleWithTracker(withReplacement, num, seed).map(_.value)
  }
}

object WrappedRDD {

  implicit def rddToPairRDDFunctions[K, V](rdd: WrappedRDD[(K, V)])(
      implicit kt: ClassTag[K],
      vt: ClassTag[V],
      ord: Ordering[K] = null): WrappedPairRDD[K, V] = {
    new WrappedPairRDD(
      rdd
        .getUnWrappedRDD()
        .map(s =>
          (s.value._1,
           new Tracker(s.value._2, s.bitmap)))
      )
    
  }

  // TODO: jteoh observation, this can be dangerously expensive for pair-based operations which
  //  typically return the base RDD type. We may want to refactor to either consistently use
  //  Tracker(K,V) or implement a subclass of WrappedRDD that handles pairwise operations without
  //  excessive conversion.
  implicit def TrackerK_TrackerV_ToTrackerKV[K, V](
      rdd: RDD[(K, Tracker[V])]): RDD[Tracker[(K, V)]] = {
    return rdd.map(s => {
     // s._1.bitmap.or(s._2.bitmap)
      new Tracker((s._1, s._2.value), s._2.bitmap)
    })
  }
  
  implicit def trackerRDDToWrappedRDD[V: ClassTag](rdd: RDD[Tracker[V]]): WrappedRDD[V] = {
    new WrappedRDD(rdd)
  }
  
  // Scala doesn't allow for implicit conversions to chain, so we explicitly define one.
  implicit def trackerPairRDDToWrappedRDD[K,V](
    rdd: RDD[(K, Tracker[V])]): WrappedRDD[(K, V)] = {
    trackerRDDToWrappedRDD(TrackerK_TrackerV_ToTrackerKV(rdd))
  }
  
}
