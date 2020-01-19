package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.serializer.Serializer

import scala.reflect.ClassTag

trait PairProvenanceRDD[K, V] extends ProvenanceRDD[(K, V)] {
  // Require that this trait is only mixed into ProvenanceRDDs
  this: ProvenanceRDD[(K, V)] =>
  
  val kct: ClassTag[K]
  val vct: ClassTag[V]
  
  
  def defaultPartitioner: Partitioner
  
  def values: BaseProvenanceRDD[V]
  def mapValues[U: ClassTag](f: V => U): PairProvenanceRDD[K, U]
  
  def combineByKeyWithClassTag[C](
                                     createCombiner: V => C,
                                     mergeValue: (C, V) => C,
                                     mergeCombiners: (C, C) => C,
                                     partitioner: Partitioner = defaultPartitioner,
                                     mapSideCombine: Boolean = true,
                                     serializer: Serializer = null)
                                 (implicit ct: ClassTag[C]): PairProvenanceRDD[K, C]
  
  def reduceByKey(func: (V, V) => V): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag(identity, func, func)(vct)
  }
  def reduceByKey(func: (V, V) => V, numPartitions: Int): PairProvenanceRDD[K, V] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)(vct)
  }
  
  def groupByKey(partitioner: Partitioner): PairProvenanceRDD[K, ProvenanceGrouping[V]]
  def groupByKey(numPartitions: Int): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
    groupByKey(new HashPartitioner(numPartitions))
  }
  def groupByKey(): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
    groupByKey(defaultPartitioner)
  }
  
  def join[W](other: PairProvenanceDefaultRDD[K, W],
              partitioner: Partitioner = defaultPartitioner
             ): PairProvenanceRDD[K, (V, W)]
  
  
  
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
