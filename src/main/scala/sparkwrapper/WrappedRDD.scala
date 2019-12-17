package sparkwrapper


import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils
import symbolicprimitives.Tracker

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Created by malig on 12/3/19.
  */
class WrappedRDD[T: ClassTag](rdd: RDD[Tracker[T]]) extends RDD[T](rdd.sparkContext, rdd.dependencies) with
  Serializable {

  def getUnWrappedRDD(): RDD[Tracker[T]] = {
    return rdd
  }
  override def map[U: ClassTag](f: T => U): WrappedRDD[U] = {
    return new WrappedRDD(rdd.map(s => new Tracker[U](f(s.value), s.bitmap)))
  }

  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): WrappedRDD[U] = {
    return new WrappedRDD(
      rdd.flatMap(a => f(a.value).map(s => new Tracker(s, a.bitmap))))
  }

  override def filter(f: T => Boolean): WrappedRDD[T] = {
    return new WrappedRDD(rdd.filter(s => f(s.value)))
  }

  // TODO implement collect()
  def collectWithTrackers(): Array[Tracker[T]] = {
    return rdd.collect()
  }
  
  override def count(): Long= {
    return rdd.count()
  }
  
  // TODO: implement take()
  def takeWithTrackers(num: Int): Array[Tracker[T]] = {
    rdd.take(num)
  }
  
  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  override def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): WrappedRDD[T] = {
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
  }
  
  /**
   * Return a new RDD containing the distinct elements in this RDD.
   */
  override def distinct(): WrappedRDD[T] = {
    distinct(rdd.partitions.length)
  }
  
  override def cache(): this.type = {
    rdd.cache()
    this
  }
  
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw new NotImplementedError("WrappedRDD's should never be directly computed.")
  }
  
  def computeWithTracker(split: Partition, context: TaskContext): Iterator[Tracker[T]] = {
    rdd.compute(split, context)
  }
  
  override protected def getPartitions: Array[Partition] = rdd.partitions
  
  def takeSampleWithTracker(
                               withReplacement: Boolean,
                               num: Int,
                               // should technically  use Spark's Utils.random
                               seed: Long = new Random().nextLong): Array[Tracker[T]] = {
    rdd.takeSample(withReplacement, num, seed)
  }
  
  override def takeSample(
                             withReplacement: Boolean,
                             num: Int,
                             // should technically  use Spark's Utils.random
                             seed: Long = new Random().nextLong): Array[T] = {
    takeSampleWithTracker(withReplacement, num, seed).map(_.value)
  }
  
  
  override def zip[U: ClassTag](other: RDD[U]): WrappedRDD[(T, U)] = {
    val result: RDD[Tracker[(T, U)]] = rdd.zip(other).map({
      case (tracker, otherValue) =>
        new Tracker((tracker.value, otherValue), tracker.bitmap)
      }
    )
    return new WrappedRDD(result)
  }
  
  // Assumption: the zip rdd is generated from the same rdd, so we only need one set of trackers
//  def zipPartitions[B: ClassTag, V: ClassTag]
//  (rdd2: RDD[B], preservesPartitioning: Boolean)
//  (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
//    {
//      rdd.zipPartitions(rdd2)((trackerIter, zipIter) => {
//        val resultIter: Iterator[V] = f(trackerIter.map(_.value), zipIter)
//        trackerIter.map(_.bitmap).zip()
//        trackerIter.zip(zipIter).map(
//          (tracker, other) =>
//          )
//      })
////        (iter1: Iterator[Tracker[T], iter2: Iterator[B]) => iter1.mapf)
//    }
  
//
//    withScope {
//    new ZippedPartitionsRDD2(sc, sc.clean(f), this, rdd2, preservesPartitioning)
//  }
//
//  withScope {
//      zipPartitions(other, preservesPartitioning = false) { (thisIter, otherIter) =>
//        new Iterator[(T, U)] {
//          def hasNext: Boolean = (thisIter.hasNext, otherIter.hasNext) match {
//            case (true, true) => true
//            case (false, false) => false
//            case _ => throw new SparkException("Can only zip RDDs with " +
//                                                 "same number of elements in each partition")
//          }
//          def next(): (T, U) = (thisIter.next(), otherIter.next())
//        }
//      }
//    }
//  }
//
  
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
  
  // Provided so code will compile with just the SparkContext change, but we may want to consider
  // switching the existing collect() to an alternate API to better mimic Spark's typical collect()
//  implicit def collectedTrackersToValues[V](
//      trackers: Array[Tracker[V]]): Array[V] = {
//    trackers.map(_.value)
//  }
  
}
