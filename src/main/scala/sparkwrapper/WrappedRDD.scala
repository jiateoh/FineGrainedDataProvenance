package sparkwrapper

import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import symbolicprimitives.AnyTracker

import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */
class WrappedRDD[T:ClassTag](rdd : RDD[AnyTracker[T]]) extends Serializable{

  def getUnWrappedRDD(): RDD[AnyTracker[T]] ={
    return rdd
  }
  def map[U: ClassTag](f: T => U): WrappedRDD[U] = {
    return new WrappedRDD(rdd.map(s => new AnyTracker(f(s.value), s.bitmap)))
  }

  def flatMap[U: ClassTag](f: T => TraversableOnce[U]): WrappedRDD[U] = {
    return new WrappedRDD(rdd.flatMap(a =>  f(a.value).map(s => new AnyTracker(s,a.bitmap))))
  }

  def filter(f: T => Boolean): WrappedRDD[T] =  {
    return new WrappedRDD(rdd.filter(s => f(s.value)))
  }

  def collect(): Array[AnyTracker[T]] = {
    return rdd.collect()
  }
}

object  WrappedRDD {

  implicit def rddToPairRDDFunctions[K, V](rdd: WrappedRDD[(K, V)])
                                          (implicit kt: ClassTag[K], vt: ClassTag[V], ord: Ordering[K] = null): WrappedPairRDD[K, V] = {
    val pair_rdd = new PairRDDFunctions[AnyTracker[K], AnyTracker[V]] (rdd.getUnWrappedRDD().map(s =>
        ( new AnyTracker(s.value._1, s.bitmap), new AnyTracker(s.value._2, s.bitmap) )
      ))
    return new WrappedPairRDD(pair_rdd)
  }


  implicit def TrackerK_TrackerV_ToTrackerKV[K, V](rdd: RDD[(AnyTracker[K], AnyTracker[V])]) : RDD[AnyTracker[(K, V)]] = {
     return rdd.map(s => {
          s._1.bitmap.or(s._2.bitmap)
          new AnyTracker( (s._1.value, s._2.value), s._1.bitmap)
        }
      )
  }

}