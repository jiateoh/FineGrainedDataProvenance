package provenance.rdd

import org.apache.spark.Partitioner.defaultPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, Partitioner}
import provenance.Provenance
import sparkwrapper.CompactBuffer

import scala.reflect.ClassTag

class PairProvenanceRDD[K: ClassTag, V: ClassTag](val rdd: RDD[(K, ProvenanceRow[V])])
  extends BaseProvenanceRDD[(K, V)](rdd) {
  
  
  private def flatRDD: RDD[ProvenanceRow[(K,V)]] = {
    rdd.map({
      case (k, (v, prov)) => ((k, v), prov)
    })
  }
  
  private def flatProvenanceRDD: FlatProvenanceRDD[(K,V)] = {
    FlatProvenanceRDD
      .pairToFlat(this)
  }
  
  def values: FlatProvenanceRDD[V] = {
    new FlatProvenanceRDD[V](rdd.values)
  }
  
  override def map[U: ClassTag](f: ((K, V)) => U): FlatProvenanceRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceRDD(rdd.map({
      case (k, (v, prov)) => (f((k, v)), prov)
    }))
  }
  
  def mapValues[U: ClassTag](f: V => U): PairProvenanceRDD[K, U] = {
    new PairProvenanceRDD(rdd.mapValues({
      case (v, prov) => (f(v), prov)
    }))
  }
  
  override def flatMap[U: ClassTag](f: ((K, V)) => TraversableOnce[U]): FlatProvenanceRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceRDD(rdd.flatMap({
      case (k, (v, prov)) => f((k, v)).map((_, prov))
    }))
  }
  
  override def filter(f: ((K, V)) => Boolean): ProvenanceRDD[(K, V)] = {
    // filter doesn't require us to remap anything, so keep it as a pair rdd
    new PairProvenanceRDD(rdd.filter({
      case (k, (v, _)) => f((k, v))
    }))
  }
  
  override def distinct(numPartitions: Int)
                       (implicit ord: Ordering[(K, V)]): ProvenanceRDD[(K, V)
  ] = map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
  
  override def collect(): Array[(K, V)] = flatProvenanceRDD.collect()
  
  override def collectWithProvenance(): Array[((K, V), Provenance)] = flatProvenanceRDD.collectWithProvenance()
  
  override def take(num: Int): Array[(K, V)] = flatProvenanceRDD.take(num)
  
  override def takeWithProvenance(num: Int): Array[((K, V), Provenance)] =
    flatProvenanceRDD.takeWithProvenance(num)
  
  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[(K, V)] =
    flatProvenanceRDD.takeSample(withReplacement, num, seed)
  
  override def takeSampleWithProvenance(withReplacement: Boolean, num: Int, seed: Long): Array[((K, V), Provenance)] =
    flatProvenanceRDD.takeSampleWithProvenance(withReplacement, num, seed)
  
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
                                     serializer: Serializer = null)(implicit ct: ClassTag[C]): PairProvenanceRDD[K, C] = {
    
    new PairProvenanceRDD[K, C](
      rdd.combineByKeyWithClassTag[ProvenanceRow[C]](
        // init: create a new 'tracker' instance that we can reuse for all values in the key.
        // TODO existing bug: cloning provenance is expensive and should be done lazily...
        (valueRow: ProvenanceRow[V]) =>
          (createCombiner(valueRow._1),
            //valueRow._2.cloneProvenance()
            valueRow._2 // TODO: intentional bug in not-cloning ever
          ),
        (combinerRow: ProvenanceRow[C], valueRow: ProvenanceRow[V]) => {
          ( mergeValue(combinerRow._1, valueRow._1),
            combinerRow._2.merge(valueRow._2)
          )
        },
        (combinerRow1: ProvenanceRow[C], combinerRow2: ProvenanceRow[C]) => {
          ( mergeCombiners(combinerRow1._1, combinerRow2._1),
            combinerRow1._2.merge(combinerRow2._2)
          )
        },
        partitioner,
        mapSideCombine,
        serializer
        ))
  }
  
  def reduceByKey(func: (V, V) => V): PairProvenanceRDD[K, V] = {
    combineByKeyWithClassTag(identity, func, func)
  }
  
  
  /**
    * Merge the values for each key using an associative and commutative reduce function. This will
    * also perform the merging locally on each mapper before sending results to a reducer, similarly
    * to a "combiner" in MapReduce. Output will be hash-partitioned with numPartitions partitions.
    */
  def reduceByKey(func: (V, V) => V, numPartitions: Int): PairProvenanceRDD[K, V] = {
    reduceByKey(new HashPartitioner(numPartitions), func)
  }
  
  /**
    * Merge the values for each key using an associative and commutative reduce function. This will
    * also perform the merging locally on each mapper before sending results to a reducer, similarly
    * to a "combiner" in MapReduce.
    */
  def reduceByKey(partitioner: Partitioner, func: (V, V) => V): PairProvenanceRDD[K, V] =  {
    combineByKeyWithClassTag[V]((v: V) => v, func, func, partitioner)
  }
  
  // END: Additional Spark-supported reduceByKey APIs
  
  
  def groupByKey(partitioner: Partitioner): PairProvenanceRDD[K, Iterable[V]] =  {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs: PairProvenanceRDD[K, CompactBuffer[V]] = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    
    // Final cast of CompactBuffer -> Iterable for API matching
    bufs.asInstanceOf[PairProvenanceRDD[K, Iterable[V]]]
  }
  
  def groupByKey(numPartitions: Int): PairProvenanceRDD[K, Iterable[V]] = {
    groupByKey(new HashPartitioner(numPartitions))
  }
  
  def groupByKey(): PairProvenanceRDD[K, Iterable[V]] = {
    groupByKey(defaultPartitioner(rdd))
  }
  
  /** Join two RDDs while maintaining the key-key lineage. This operation is currently only
    * supported for RDDs that possess the same base input RDD.
    */
  def join[W](other: PairProvenanceRDD[K, W],
              partitioner: Partitioner = defaultPartitioner(rdd)
             ): PairProvenanceRDD[K, (V, W)] = {
    assert(rdd.firstSource == other.rdd.firstSource,
           "Provenance-based join is currently supported only for RDDs originating from the same " +
             "input data (e.g. self-join): " + s"${rdd.firstSource} vs. ${other.rdd.firstSource}")
    val value: RDD[(K, (Iterable[(V, Provenance)], Iterable[(W, Provenance)]))] = rdd.cogroup(other.rdd)
    val result: RDD[(K, ProvenanceRow[(V, W)])] = rdd.cogroup(other.rdd).flatMapValues(pair =>
           for (thisRow <- pair._1.iterator; otherRow <- pair._2.iterator)
             yield ((thisRow._1, otherRow._1), thisRow._2.cloneProvenance().merge(otherRow._2))
      )
    new PairProvenanceRDD(result)
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
