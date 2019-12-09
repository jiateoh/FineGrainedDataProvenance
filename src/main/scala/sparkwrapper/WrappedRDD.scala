package sparkwrapper


import org.apache.spark.rdd.RDD
import symbolicprimitives.Tracker

import scala.reflect.ClassTag

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

  implicit def TrackerK_TrackerV_ToTrackerKV[K, V](
      rdd: RDD[(K, Tracker[V])]): RDD[Tracker[(K, V)]] = {
    return rdd.map(s => {
     // s._1.bitmap.or(s._2.bitmap)
      new Tracker((s._1, s._2.value), s._2.bitmap)
    })
  }

}
