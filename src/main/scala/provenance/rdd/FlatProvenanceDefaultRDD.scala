package provenance.rdd

import org.apache.spark.rdd.RDD
import provenance.data.Provenance
import symbolicprimitives.Utils

import scala.reflect.ClassTag

class FlatProvenanceDefaultRDD[T: ClassTag](val rdd: RDD[ProvenanceRow[T]]) extends
  BaseProvenanceRDD[T](rdd) {
  
  private def rddWithoutProvenance: RDD[T] = rdd.map(_._1)
  
  override def map[U: ClassTag](f: T  => U , enableUDFAwareProv: Boolean = true): FlatProvenanceDefaultRDD[U] =
    new FlatProvenanceDefaultRDD(rdd.map {
      row => Utils.computeOneToOneUDF(f,row,enableUDFAwareProv)
    })

  
  override def flatMap[U  : ClassTag](f: T => TraversableOnce[U], enableUDFAwareProv: Boolean = true): FlatProvenanceDefaultRDD[U] = {
    new FlatProvenanceDefaultRDD(rdd.flatMap{
      // TODO this might be slow, one optimization is to have a classTag on the return type and
      // check that ahead of time before creating the UDF
      row => Utils.computeOneToManyUDF(f,row, enableUDFAwareProv)
//        resultTraversable match {
//          case provenanceGroup: ProvenanceGrouping[U] =>
//            // TODO undo this as it's ignoring optimized provenance
//            //provenanceGroup.getData
//            provenanceGroup.getData.map(pair => (pair._1, prov))
//         case _ =>
//            if(!enableUDFAwareProv)
//              resultTraversable.map{(_, prov)}
//            else
//              resultTraversable.map{
//                case row : SymBase => (row,row.getProvenance())
//                case _ => throw new UnsupportedOperationException(
//                  "UDFAwareProvenance is enables but the returned object is not SymBase")
//              }
//        }
      //}
    })
  }
  
  //  override def flatMap[U: ClassTag](f: T => ProvenanceGrouping[U]): FlatProvenanceDefaultRDD[U] = {
//    // If a provenance grouping is returned, we should expect to flatten it ourselves and split
//    // up the provenance accordingly.
//    // There's an unstated assumption here that the arguments (K, V) contain a base
//    // provenance grouping and an operation such as map() is being called on them.
//    // As a result, the provided provenance is unused (e.g. it may have been the merged
//    // provenance for the entire ProvenanceGrouping, used as a placeholder in case it's needed
//    // later).
//    new FlatProvenanceDefaultRDD(rdd.flatMap({
//      case (inp, unusedProvenance) => f(inp).asIterable
//    }))
//  }

  override def collect(): Array[T] = rddWithoutProvenance.collect()

  override def collectWithProvenance(): Array[ProvenanceRow[T]] = rdd.collect()

  override def filter(f: T => Boolean): ProvenanceRDD[T] =
    new FlatProvenanceDefaultRDD(rdd.filter(row => f(row._1)))
//
}

object FlatProvenanceDefaultRDD {
  implicit def flatToPair[K: ClassTag, V: ClassTag](flatRdd: FlatProvenanceDefaultRDD[(K,V)])
  : PairProvenanceDefaultRDD[K,V] = {
    new PairProvenanceDefaultRDD[K,V](
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
  implicit def pairToFlat[K: ClassTag, V:ClassTag](pairRdd: PairProvenanceDefaultRDD[K,V])
  : FlatProvenanceDefaultRDD[(K,V)] = {
    new FlatProvenanceDefaultRDD[(K, V)](pairRdd.rdd.map({
      case (k: K, (v: V, prov: Provenance)) => ((k, v), prov)
      })
                                         )
  }
}
