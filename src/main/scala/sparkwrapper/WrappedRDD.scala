package sparkwrapper


import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import trackers.BaseTracker

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by malig on 12/3/19.
  */
class WrappedRDD[T: ClassTag](val rdd: RDD[BaseTracker[T]]) extends Serializable {
  
  def map[U: ClassTag](f: T => U): WrappedRDD[U] = {
    new WrappedRDD(rdd.map(s => s.withValue(f(s.value))))
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): WrappedRDD[U] = {
     new WrappedRDD(
      rdd.flatMap(a => f(a.value).map(a.withValue)))
  }

  def filter(f: T => Boolean): WrappedRDD[T] = {
    new WrappedRDD(rdd.filter(s => f(s.value)))
  }

  def collect(): Array[BaseTracker[T]] = rdd.collect()
  
  def count():Long= rdd.count()
  
  def take(num: Int): Array[BaseTracker[T]] = rdd.take(num)
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
                            seed: Long = new Random().nextLong): Array[BaseTracker[T]] = {
    rdd.takeSample(withReplacement, num, seed)
  }
  
  def takeSample(withReplacement: Boolean,
                 num: Int,
                 // should technically  use Spark's Utils.random
                 seed: Long = new Random().nextLong): Array[T] = {
    takeSampleWithTracker(withReplacement, num, seed).map(_.value)
  }
  
  def setName(name: String): this.type = {
    rdd.setName(name)
    this
  }
}

object WrappedRDD {

  implicit def rddToPairRDDFunctions[K, V](wrapped: WrappedRDD[(K, V)])(
      implicit kt: ClassTag[K],
      vt: ClassTag[V],
      ord: Ordering[K] = null): WrappedPairRDD[K, V] = {
    new WrappedPairRDD(
      wrapped.rdd
        .map(s =>
          (s.value._1,
            s.withValue(s.value._2)))
      )
    
  }

  // TODO: jteoh observation, this can be dangerously expensive for pair-based operations which
  //  typically return the base RDD type. We may want to refactor to either consistently use
  //  Tracker(K,V) or implement a subclass of WrappedRDD that handles pairwise operations without
  //  excessive conversion.
  // TODO: 1/7/2020: This is called in 3 places
  implicit def TrackerK_TrackerV_ToTrackerKV[K, V](
      rdd: RDD[(K, BaseTracker[V])]): RDD[BaseTracker[(K, V)]] = {
    rdd.map(s => {
      s._2.withValue((s._1, s._2.value))
    })
  }
  
  implicit def trackerRDDToWrappedRDD[V: ClassTag](rdd: RDD[BaseTracker[V]]): WrappedRDD[V] =  {
    new WrappedRDD(rdd)
  }
  
  // Scala doesn't allow for implicit conversions to chain, so we explicitly define one.
  implicit def trackerPairRDDToWrappedRDD[K,V](
    rdd: RDD[(K, BaseTracker[V])]): WrappedRDD[(K, V)] = {
    trackerRDDToWrappedRDD(TrackerK_TrackerV_ToTrackerKV(rdd))
  }
  
}
