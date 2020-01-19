package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, Partitioner}
import provenance.Provenance
import sparkwrapper.CompactBuffer

import scala.reflect.ClassTag

class PairProvenanceDefaultRDD[K, V](val rdd: RDD[(K, ProvenanceRow[V])])
                                    (implicit val kct: ClassTag[K],
                                     implicit val vct: ClassTag[V])
  extends BaseProvenanceRDD[(K, V)](rdd) with PairProvenanceRDD[K,V] {
  
  override def defaultPartitioner: Partitioner =
    org.apache.spark.Partitioner.defaultPartitioner(rdd)
  
  private def flatRDD: RDD[ProvenanceRow[(K,V)]] = {
    rdd.map({
      case (k, (v, prov)) => ((k, v), prov)
    })
  }
  
  private def flatProvenanceRDD: FlatProvenanceDefaultRDD[(K,V)] = {
    FlatProvenanceDefaultRDD
      .pairToFlat(this)
  }
  
  override def values: FlatProvenanceDefaultRDD[V] = {
    new FlatProvenanceDefaultRDD[V](rdd.values)
  }
  
  override def map[U: ClassTag](f: ((K, V)) => U): FlatProvenanceDefaultRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceDefaultRDD(rdd.map({
      case (k, (v, prov)) => (f((k, v)), prov)
    }))
  }
  
  override def mapValues[U: ClassTag](f: V => U): PairProvenanceDefaultRDD[K, U] = {
    new PairProvenanceDefaultRDD(rdd.mapValues({
      case (v, prov) => (f(v), prov)
    }))
  }
  
  override def flatMap[U: ClassTag](f: ((K, V)) => TraversableOnce[U]): FlatProvenanceDefaultRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceDefaultRDD(rdd.flatMap({
          // TODO this might be slow, one optimization is to have a classTag on the return type and
          // check that ahead of time before creating the UDF
      case (k, (v, prov)) => {
        val resultTraversable = f((k, v))
        resultTraversable match {
          case provenanceGroup: ProvenanceGrouping[U] =>
            provenanceGroup.getData
          case _ =>
            resultTraversable.map((_, prov))
        }
      }
    }))
  }
  
  /** Specialized flatMap to detect if a ProvenanceGrouping is used. */
//  override def flatMap[U: ClassTag](f: ((K, V)) => ProvenanceGrouping[U]): FlatProvenanceDefaultRDD[U] = {
//    // If a provenance grouping is returned, we should expect to flatten it ourselves and split
//    // up the provenance accordingly.
//    // There's an unstated assumption here that the arguments (K, V) contain a base
//    // provenance grouping and an operation such as map() is being called on them.
//    // As a result, the provided provenance is unused (e.g. it may have been the merged
//    // provenance for the entire ProvenanceGrouping, used as a placeholder in case it's needed
//    // later).
//    new FlatProvenanceDefaultRDD(rdd.flatMap({
//      case (k, (v, unusedProvenance)) => f((k, v)).asIterable
//    }))
//  }
  
  override def filter(f: ((K, V)) => Boolean): ProvenanceRDD[(K, V)] = {
    // filter doesn't require us to remap anything, so keep it as a pair rdd
    new PairProvenanceDefaultRDD(rdd.filter({
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
  override def combineByKeyWithClassTag[C](
                                     createCombiner: V => C,
                                     mergeValue: (C, V) => C,
                                     mergeCombiners: (C, C) => C,
                                     partitioner: Partitioner = defaultPartitioner,
                                     mapSideCombine: Boolean = true,
                                     serializer: Serializer = null)(implicit ct: ClassTag[C]): PairProvenanceDefaultRDD[K, C] = {
    
    new PairProvenanceDefaultRDD[K, C](
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
  
  // END: Additional Spark-supported reduceByKey APIs
  
  // An alternate GBK that produces a specialized 'iterable' which internally tracks fine-grained
  // provenance.
//  override def groupByKey(partitioner: Partitioner): PairProvenanceGroupingRDD[K, V] =  {
//    // groupByKey shouldn't use map side combine because map side combine does not
//    // reduce the amount of data shuffled and requires all map side data be inserted
//    // into a hash table, leading to more objects in the old gen.
//    val createCombiner = (v: ProvenanceRow[V]) => CompactBuffer(v)
//    val mergeValue = (buf: CompactBuffer[ProvenanceRow[V]], v: ProvenanceRow[V]) => buf += v
//    val mergeCombiners = (c1: CompactBuffer[ProvenanceRow[V]], c2: CompactBuffer[ProvenanceRow[V]]) => c1 ++= c2
//    val bufs: RDD[(K, CompactBuffer[ProvenanceRow[V]])] =
//      rdd.combineByKeyWithClassTag[CompactBuffer[ProvenanceRow[V]]](
//        createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
//
//    val underlyingResult: RDD[(K, ProvenanceGrouping[V])] =
//      bufs.mapValues(buf => new ProvenanceGrouping(buf))
//    new PairProvenanceGroupingRDD(underlyingResult)
//  }
//
//  override def groupByKey(numPartitions: Int): PairProvenanceGroupingRDD[K, V] = {
//    groupByKey(new HashPartitioner(numPartitions))
//  }
//
//  override def groupByKey(): PairProvenanceGroupingRDD[K, V] = {
//    groupByKey(defaultPartitioner)
//  }
  
  // This looks like the naive approach, but returns a ProvenanceGrouping
  override def groupByKey(partitioner: Partitioner): PairProvenanceRDD[K, ProvenanceGrouping[V]]  =  {
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: ProvenanceRow[V]) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[ProvenanceRow[V]], v: ProvenanceRow[V]) => buf += v
    val mergeCombiners = (c1: CompactBuffer[ProvenanceRow[V]], c2: CompactBuffer[ProvenanceRow[V]]) => c1 ++= c2
    val bufs: RDD[(K, CompactBuffer[ProvenanceRow[V]])] =
      rdd.combineByKeyWithClassTag[CompactBuffer[ProvenanceRow[V]]](
        createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
    
    val underlyingResult: RDD[(K, (ProvenanceGrouping[V], Provenance))] =
      bufs.mapValues(buf => {
        val group = new ProvenanceGrouping(buf)
        (group, group.combinedProvenance)
      })
    new PairProvenanceDefaultRDD(underlyingResult)
  }
  
  override def groupByKey(numPartitions: Int): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
    groupByKey(new HashPartitioner(numPartitions))
  }
  
  override def groupByKey(): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
    groupByKey(defaultPartitioner)
  }
  
  def groupByKeyNaive(partitioner: Partitioner): PairProvenanceDefaultRDD[K, Iterable[V]] =  {
  
    // groupByKey shouldn't use map side combine because map side combine does not
    // reduce the amount of data shuffled and requires all map side data be inserted
    // into a hash table, leading to more objects in the old gen.
    val createCombiner = (v: V) => CompactBuffer(v)
    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
    val bufs: PairProvenanceDefaultRDD[K, CompactBuffer[V]] = combineByKeyWithClassTag[CompactBuffer[V]](
      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
  
    // Final cast of CompactBuffer -> Iterable for API matching
    bufs.asInstanceOf[PairProvenanceDefaultRDD[K, Iterable[V]]]
  }
  
  def groupByKeyNaive(numPartitions: Int): PairProvenanceDefaultRDD[K, Iterable[V]] = {
    groupByKeyNaive(new HashPartitioner(numPartitions))
  }
  
  def groupByKeyNaive(): PairProvenanceDefaultRDD[K, Iterable[V]] = {
    groupByKeyNaive(defaultPartitioner)
  }
  
  /** Join two RDDs while maintaining the key-key lineage. This operation is currently only
    * supported for RDDs that possess the same base input RDD.
    */
  override def join[W](other: PairProvenanceDefaultRDD[K, W],
                       partitioner: Partitioner = defaultPartitioner
             ): PairProvenanceDefaultRDD[K, (V, W)] = {
    assert(rdd.firstSource == other.rdd.firstSource,
           "Provenance-based join is currently supported only for RDDs originating from the same " +
             "input data (e.g. self-join): " + s"${rdd.firstSource} vs. ${other.rdd.firstSource}")
    val value: RDD[(K, (Iterable[(V, Provenance)], Iterable[(W, Provenance)]))] = rdd.cogroup(other.rdd)
    val result: RDD[(K, ProvenanceRow[(V, W)])] = rdd.cogroup(other.rdd).flatMapValues(pair =>
           for (thisRow <- pair._1.iterator; otherRow <- pair._2.iterator)
             yield ((thisRow._1, otherRow._1), thisRow._2.cloneProvenance().merge(otherRow._2))
      )
    new PairProvenanceDefaultRDD(result)
  }
  
}
