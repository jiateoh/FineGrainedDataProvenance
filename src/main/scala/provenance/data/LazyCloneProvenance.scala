package provenance.data

// Class representing a clone of another provenance, but one that is ideally done lazily to
// reduce memory pressure (e.g. if reduceByKey with only one record per key).
class LazyCloneProvenance(val orig: Provenance) extends Provenance {
  override def hashCode(): Int = orig.hashCode()
  
  override def equals(obj: Any): Boolean = orig.equals(obj)
  
  override def toString: String = s"*${orig.toString} (lazy clone)"
  override def cloneProvenance(): Provenance = this
  
  /** Merges two provenance instances (in place), returning the current instance after merging. */
  override def merge(other: Provenance): Provenance = orig.cloneProvenance().merge(other)
  
  /** Returns number of provenance IDs. */
  override def count: Int = orig.count
  
  /** Returns estimate in serialization size, experimental. */
  override def estimateSize: Long = orig.estimateSize
}
