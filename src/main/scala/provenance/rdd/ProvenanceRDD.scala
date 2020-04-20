package provenance.rdd

import org.apache.spark.storage.StorageLevel
import symbolicprimitives.SymBase

import scala.reflect.ClassTag
import scala.util.Random


/** Trait to ensure consistent base API between Pair and non-Pair */
trait ProvenanceRDD[T] extends Serializable {
  def map[U: ClassTag](f: T => U, enableUDFAwareProv: Option[Boolean] = None): ProvenanceRDD[U]
  
  def flatMap[U:ClassTag](f: T => TraversableOnce[U], enableUDFAwareProv: Option[Boolean] = None): ProvenanceRDD[U]
  
  def filter(f: T => Boolean): ProvenanceRDD[T]
//
//  def count(): Long
//
//  def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): ProvenanceRDD[T]
//
//  def distinct(): ProvenanceRDD[T]
//
//  def persist(newLevel: StorageLevel): this.type
//
//  def persist(): this.type
//
//  def unpersist(blocking: Boolean = true): this.type
//
//  def cache(): this.type
//
 def collect(): Array[T]
//
  def collectWithProvenance(): Array[ProvenanceRow[T]]

  def take(num: Int): Array[T]

  def takeWithProvenance(num: Int): Array[ProvenanceRow[T]]
//
//  def takeSample(withReplacement: Boolean,
//                 num: Int,
//                 // should technically  use Spark's Utils.random
//                 seed: Long = new Random().nextLong): Array[T]
//
//  def takeSampleWithProvenance(withReplacement: Boolean,
//                 num: Int,
//                 // should technically  use Spark's Utils.random
//                 seed: Long = new Random().nextLong): Array[ProvenanceRow[T]]
//
  
  def setName(name: String): this.type
  
}

object ProvenanceRDD {
  implicit def toPairRDD[K: ClassTag, V: ClassTag](rdd: ProvenanceRDD[(K,V)]): PairProvenanceDefaultRDD[K,V] = {
    rdd match {
      case pair: PairProvenanceDefaultRDD[K, V] => pair
      case flat: FlatProvenanceDefaultRDD[(K, V)] => FlatProvenanceDefaultRDD.flatToPair(flat)
      case _ => throw new NotImplementedError("Unknown RDD type for pair conversion: $rdd")
    }
  }
}
