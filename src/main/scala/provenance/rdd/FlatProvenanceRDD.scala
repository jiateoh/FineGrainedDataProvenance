package provenance.rdd

import org.apache.spark.rdd.RDD
import provenance.Provenance

import scala.reflect.ClassTag

class FlatProvenanceRDD[T: ClassTag](val rdd: RDD[ProvenanceRow[T]]) extends
  BaseProvenanceRDD[T](rdd) {
  
  private def rddWithoutProvenance: RDD[T] = rdd.map(_._1)
  
  override def map[U: ClassTag](f: T => U): FlatProvenanceRDD[U] =
    new FlatProvenanceRDD(rdd.map(row => (f(row._1), row._2)))
  
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): FlatProvenanceRDD[U] =
    new FlatProvenanceRDD(rdd.flatMap(row => f(row._1).map((_, row._2))))
  
  override def filter(f: T => Boolean): ProvenanceRDD[T] =
    new FlatProvenanceRDD(rdd.filter(row => f(row._1)))
  
  override def distinct(numPartitions: Int)(implicit ord: Ordering[T]): ProvenanceRDD[T] =
    map(x => (x, null)).reduceByKey((x, y) => x, numPartitions).map(_._1)
  
  override def collect(): Array[T] = rddWithoutProvenance.collect()
  
  override def collectWithProvenance(): Array[ProvenanceRow[T]] = rdd.collect()
  
  override def take(num: Int): Array[T] = rddWithoutProvenance.take(num)
  
  override def takeWithProvenance(num: Int): Array[ProvenanceRow[T]] = rdd.take(num)
  
  override def takeSample(withReplacement: Boolean,
                                        num: Int,
                                        seed: Long): Array[T] =
    rddWithoutProvenance.takeSample(withReplacement, num, seed)
  
  override def takeSampleWithProvenance(withReplacement: Boolean,
                                        num: Int,
                                        seed: Long): Array[ProvenanceRow[T]] =
    rdd.takeSample(withReplacement, num, seed)
  
}

object FlatProvenanceRDD {
  implicit def flatToPair[K: ClassTag, V: ClassTag](flatRdd: FlatProvenanceRDD[(K,V)])
  : PairProvenanceRDD[K,V] = {
    new PairProvenanceRDD[K,V](
      flatRdd.rdd.map(
        //((kv: (K, V), prov: Provenance)) => {
        {case (kv: (K, V), prov: Provenance) => (kv._1, (kv._2, prov))}
//        (kv._1, (kv._2, prov))
      )
//      flatRdd.rdd.map({
//        case ((k: K,v: V), prov: Provenance) => (k, (v, prov))
//      })
    )
  }
  
  // Commented out because ideally we shouldn't need this
  implicit def pairToFlat[K: ClassTag, V:ClassTag](pairRdd: PairProvenanceRDD[K,V])
  : FlatProvenanceRDD[(K,V)] = {
    new FlatProvenanceRDD[(K, V)](pairRdd.rdd.map({
      case (k: K, (v: V, prov: Provenance)) => ((k, v), prov)
      })
    )
  }
}
