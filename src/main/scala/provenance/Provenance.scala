package provenance

trait Provenance extends Serializable {
  
  // Renamed because clone() is a built-in function.
  def cloneProvenance(): Provenance
  
  /** Merges two provenance instances (in place), returning the current instance after merging. */
  def merge(other: Provenance): this.type 
  
  /** Returns number of provenance IDs. */
  def count: Int
  
  /** Returns estimate in serialization size, experimental. */
  def estimateSize: Long
  
}
