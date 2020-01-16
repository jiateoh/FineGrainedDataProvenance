package provenance.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.reflect.ClassTag

abstract class BaseProvenanceRDD[T: ClassTag](baseRDD: RDD[_]) extends ProvenanceRDD[T] {
  
  final def count(): Long = baseRDD.count()
  
  final def distinct(): ProvenanceRDD[T] = this.distinct(baseRDD.getNumPartitions)
  
  final def persist(newLevel: StorageLevel): this.type = {
    baseRDD.persist(newLevel)
    this
  }
  
  final def persist(): this.type = {
    baseRDD.persist()
    this
  }
  
  final def unpersist(blocking: Boolean): this.type = {
    baseRDD.unpersist(blocking)
    this
  }
  
  final def cache(): this.type = {
    baseRDD.cache()
    this
  }
  
  final def setName(name: String): this.type = {
    baseRDD.setName(name)
    this
  }
}
