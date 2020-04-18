package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.{HashPartitioner, Partitioner, SparkEnv}
import provenance.data.InfluenceMarker._
import provenance.data.{InfluenceMarker, Provenance}
import symbolicprimitives.{SymBase, Utils}
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.reflect.ClassTag

class PairProvenanceDefaultRDD[K, V](val rdd: RDD[(K, ProvenanceRow[V])])(
    implicit val kct: ClassTag[K],
    implicit val vct: ClassTag[V])
    extends BaseProvenanceRDD[(K, V)](rdd)
    with PairProvenanceRDD[K, V] {

  override def defaultPartitioner: Partitioner =
    org.apache.spark.Partitioner.defaultPartitioner(rdd)

  private def flatRDD: RDD[ProvenanceRow[(K, V)]] = {
    rdd.map({
      case (k, (v, prov)) => ((k, v), prov)
    })
  }

  private def flatProvenanceRDD: FlatProvenanceDefaultRDD[(K, V)] = {
    FlatProvenanceDefaultRDD
      .pairToFlat(this)
  }

  override def values: FlatProvenanceDefaultRDD[V] = {
    new FlatProvenanceDefaultRDD[V](rdd.values)
  }

  override def map[U: ClassTag](
      f: ((K, V)) => U,
      enableUDFAwareProv: Boolean = true): FlatProvenanceDefaultRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceDefaultRDD(rdd.map({
      case (k, (v, prov)) => (f((k, v)), prov)
    }))
  }

  override def mapValues[U: ClassTag](
      f: V => U,
      enableUDFAwareProv: Boolean = true): PairProvenanceDefaultRDD[K, U] = {
    new PairProvenanceDefaultRDD(rdd.mapValues({
      case (v, prov) =>
        Utils.computeOneToOneUDF(f, (v, prov), enableUDFAwareProv)
    }))
  }

  override def flatMap[U: ClassTag](
      f: ((K, V)) => TraversableOnce[U],
      enableUDFAwareProv: Boolean = true): FlatProvenanceDefaultRDD[U] = {
    // TODO: possible optimization if result is still a pair rdd?
    new FlatProvenanceDefaultRDD(rdd.flatMap({
      // TODO this might be slow, one optimization is to have a classTag on the return type and
      // check that ahead of time before creating the UDF
      case (k, (v, prov)) => {
        Utils.computeOneToManyUDF(f, ((k, v), prov), enableUDFAwareProv)
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
//
//  override def distinct(numPartitions: Int)
//                       (implicit ord: Ordering[(K, V)]): ProvenanceRDD[(K, V)
//  ] = map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)
//
  override def collect(): Array[(K, V)] = flatProvenanceRDD.collect()
//
  override def collectWithProvenance(): Array[((K, V), Provenance)] =
    flatProvenanceRDD.collectWithProvenance()

//  override def take(num: Int): Array[(K, V)] = flatProvenanceRDD.take(num)
//
//  override def takeWithProvenance(num: Int): Array[((K, V), Provenance)] =
//    flatProvenanceRDD.takeWithProvenance(num)
//
//  override def takeSample(withReplacement: Boolean, num: Int, seed: Long): Array[(K, V)] =
//    flatProvenanceRDD.takeSample(withReplacement, num, seed)
//
//  override def takeSampleWithProvenance(withReplacement: Boolean, num: Int, seed: Long): Array[((K, V), Provenance)] =
//    flatProvenanceRDD.takeSampleWithProvenance(withReplacement, num, seed)
//
//  /**
//    * Optimized combineByKey implementation where provenance tracking data structures are
//    * constructed only once per partition+key.
//    */


  /**
    * [Gulzar]
    * Ugly implementation of this combiner method. Following are the assumptions:
    *   1. UDFAwareProvenance is enabled by default
    *      Will only be performed when both input and output of UDF is SYM*Object
    *      if enabled, the influence function will be disabled
    *   2. Influence function will only work when UDFAwareProvenance is disabled.
    *
    *   Our modified create Combiner method (Defined in Utils) create a combiner
    *   object that contains the Combined output, the most influential source value
    *   (random value if influential function is not given), and prov of most
    *   influence/logical lineage.
    *
    *   CombinerWithInfluence --> ( (C , V), Prov)
    *
    * */

  override def combineByKeyWithClassTag[C](
      createCombiner: V => C,
      mergeValue: (C, V) => C,
      mergeCombiners: (C, C) => C,
      partitioner: Partitioner = defaultPartitioner,
      mapSideCombine: Boolean = true,
      serializer: Serializer = null,
      enableUDFAwareProv: Boolean = true,
      inflFunction: Option[(V, V) => InfluenceMarker] = None)(
      implicit ct: ClassTag[C]): PairProvenanceDefaultRDD[K, C] = {
    // Based on ShuffledRDD implementation for serializer
    val resultSerializer =
      serializer

    val createProvCombiner =
      (valueRow: ProvenanceRow[V]) =>
        Utils.createCombinerForReduce(createCombiner,valueRow._1,valueRow._2.cloneProvenance(),enableUDFAwareProv)

    val mergeProvValue =
      (combinerRow: ProvenanceRow[CombinerWithInfluence[C,V]], valueRow: ProvenanceRow[V]) => {
        Utils.computeCombinerWithValueUDF(mergeValue,
                                 combinerRow,
                                 valueRow,
                                 enableUDFAwareProv,
                                 inflFunction)
      }

    val mergeProvCombiners =
      (combinerRow1: ProvenanceRow[CombinerWithInfluence[C,V]], combinerRow2: ProvenanceRow[CombinerWithInfluence[C,V]]) => {
        Utils.computeCombinerWithCombinerUDF[C,V](mergeCombiners,
                                 combinerRow1,
                                 combinerRow2,
                                 enableUDFAwareProv,
                                 inflFunction)
      }

    new PairProvenanceDefaultRDD[K, C](
      rdd.combineByKeyWithClassTag[ProvenanceRow[CombinerWithInfluence[C,V]]](
        // init: create a new 'tracker' instance that we can reuse for all values in the key.
        // TODO existing bug: cloning provenance is expensive and should be done lazily...
        createProvCombiner,
        mergeProvValue,
        mergeProvCombiners,
        partitioner,
        mapSideCombine,
        resultSerializer
      ).map(row => (row._1 , (row._2._1._1, row._2._2)))
    )
  }

 /**
   * Moving from jteoh branch
   *
   * */
 override def aggregateByKey[U: ClassTag](zeroValue: U, partitioner: Partitioner)
                                         (seqOp: (U, V) => U,
                                          combOp: (U, U) => U): PairProvenanceRDD[K,U] = {

   // Serialize the zero value to a byte array so that we can get a new clone of it on each key
   val zeroBuffer = SparkEnv.get.serializer.newInstance().serialize(zeroValue)
   val zeroArray = new Array[Byte](zeroBuffer.limit)
   zeroBuffer.get(zeroArray)

   lazy val cachedSerializer = SparkEnv.get.serializer.newInstance()
   val createZero = () => cachedSerializer.deserialize[U](ByteBuffer.wrap(zeroArray))

   // We will clean the combiner closure later in `combineByKey`
   val cleanedSeqOp = seqOp // TODO: clean closure
   // rdd.context.clean(seqOp)
   combineByKeyWithClassTag[U]((v: V) => cleanedSeqOp(createZero(), v),
     cleanedSeqOp, combOp, partitioner)
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

//  // This looks like the naive approach, but returns a ProvenanceGrouping
//  override def groupByKey(partitioner: Partitioner): PairProvenanceRDD[K, ProvenanceGrouping[V]]  =  {
//    // groupByKey shouldn't use map side combine because map side combine does not
//    // reduce the amount of data shuffled and requires all map side data be inserted
//    // into a hash table, leading to more objects in the old gen.
//    val createCombiner = (v: ProvenanceRow[V]) => CompactBuffer(v)
//    val mergeValue = (buf: CompactBuffer[ProvenanceRow[V]], v: ProvenanceRow[V]) => buf += v
//    val mergeCombiners = (c1: CompactBuffer[ProvenanceRow[V]], c2: CompactBuffer[ProvenanceRow[V]]) => c1 ++= c2
//    val serializer = if(Provenance.useDedupSerializer) {
//      val baseSerializer = SparkEnv.get.serializerManager.getSerializer(implicitly[ClassTag[K]], implicitly[ClassTag[V]])
//      new ProvenanceDeduplicationSerializer(baseSerializer, partitioner)
//    } else {
//      null // null is the default argument value used, so it's safe here.
//    }
//
//    val bufs: RDD[(K, CompactBuffer[ProvenanceRow[V]])] =
//      rdd.combineByKeyWithClassTag[CompactBuffer[ProvenanceRow[V]]](
//        createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false,
//        serializer = serializer)
//
//    val underlyingResult: RDD[(K, (ProvenanceGrouping[V], Provenance))] =
//      bufs.mapValues(buf => {
//        val group = new ProvenanceGrouping(buf)
//        // TODO: for correctness, this provenance should be group.combinedProvenance
//        // However, for something such as pagerank, we know it is not required because it is not
//        // used later
//        // Is there a way we can leverage the DAG information or otherwise "look ahead" to
//        // determine what to do here?
//        val groupProvenance = group.combinedProvenance
//
//        (group, groupProvenance)
//      })
//    new PairProvenanceDefaultRDD(underlyingResult)
//  }

//  override def groupByKey(numPartitions: Int): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
//    groupByKey(new HashPartitioner(numPartitions))
//  }
//
//  override def groupByKey(): PairProvenanceRDD[K, ProvenanceGrouping[V]] = {
//    groupByKey(defaultPartitioner)
//  }

//  def groupByKeyNaive(partitioner: Partitioner): PairProvenanceDefaultRDD[K, Iterable[V]] =  {
//
//    // groupByKey shouldn't use map side combine because map side combine does not
//    // reduce the amount of data shuffled and requires all map side data be inserted
//    // into a hash table, leading to more objects in the old gen.
//    val createCombiner = (v: V) => CompactBuffer(v)
//    val mergeValue = (buf: CompactBuffer[V], v: V) => buf += v
//    val mergeCombiners = (c1: CompactBuffer[V], c2: CompactBuffer[V]) => c1 ++= c2
//    val bufs: PairProvenanceDefaultRDD[K, CompactBuffer[V]] = combineByKeyWithClassTag[CompactBuffer[V]](
//      createCombiner, mergeValue, mergeCombiners, partitioner, mapSideCombine = false)
//
//    // Final cast of CompactBuffer -> Iterable for API matching
//    bufs.asInstanceOf[PairProvenanceDefaultRDD[K, Iterable[V]]]
//  }
//
//  def groupByKeyNaive(numPartitions: Int): PairProvenanceDefaultRDD[K, Iterable[V]] = {
//    groupByKeyNaive(new HashPartitioner(numPartitions))
//  }
//
//  def groupByKeyNaive(): PairProvenanceDefaultRDD[K, Iterable[V]] = {
//    groupByKeyNaive(defaultPartitioner)
//  }

//  /** Join two RDDs while maintaining the key-key lineage. This operation is currently only
//    * supported for RDDs that possess the same base input RDD.
//    */
//  override def join[W](other: PairProvenanceDefaultRDD[K, W],
//                       partitioner: Partitioner = defaultPartitioner
//             ): PairProvenanceDefaultRDD[K, (V, W)] = {
//    assert(rdd.firstSource == other.rdd.firstSource,
//           "Provenance-based join is currently supported only for RDDs originating from the same " +
//             "input data (e.g. self-join): " + s"${rdd.firstSource} vs. ${other.rdd.firstSource}")
//
//    val result: RDD[(K, ProvenanceRow[(V, W)])] = rdd.cogroup(other.rdd).flatMapValues((pair: (Iterable[(V, Provenance)], Iterable[(W, Provenance)])) =>
//           for (thisRow <- pair._1.iterator; otherRow <- pair._2.iterator)
//             // TODO: enhance this provenance precision somehow.
//             // TODO: would it help to lazy merge if the two provenance objects are equivalent?
//             yield ((thisRow._1, otherRow._1), thisRow._2.cloneProvenance().merge(otherRow._2))
//                                                                                       )
//    new PairProvenanceDefaultRDD(result)
//  }
}
