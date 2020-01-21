package provenance.data

import org.apache.spark.util.SizeEstimator

import scala.collection.mutable

class SetProvenance(val data: mutable.Set[Int]) extends Provenance {
  override def cloneProvenance(): Provenance = {
    val dataCopy: mutable.Set[Int] = data.clone
    new SetProvenance(dataCopy)
  }
  
  /** Merges two provenance instances (in place), returning the current instance after merging. */
  override def merge(other: Provenance): Provenance = {
    other match {
      case otherSet: SetProvenance =>
        //this.data.addAll(otherSet.data)
        // ideally this should use addAll, but that doesn't exist for some reason.
        this.data ++= otherSet.data
      case other => throw new NotImplementedError(s"Unsupported Set merge provenance " +
                                                    s"type! $other")
    }
    this
  }
  
  /** Returns number of provenance IDs. */
  override def count: Int = data.size
  
  /** Returns estimate in serialization size, experimental. */
  override def estimateSize: Long = SizeEstimator.estimate(this)
}

object SetProvenance extends ProvenanceFactory {
  override def create(id: Long): Provenance = {
    if (id > Int.MaxValue)
      throw new UnsupportedOperationException(
        "The offset is greater than Int.Max which is not supported yet")
    val data = mutable.Set[Int](id.asInstanceOf[Int])
    new SetProvenance(data)
  }
}
