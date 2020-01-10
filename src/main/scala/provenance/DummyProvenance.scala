package provenance

class DummyProvenance extends Provenance {
  override def cloneProvenance(): Provenance = this
  
  override def merge(other: Provenance): this.type = this
  
  override def count: Int = 0
  
  override def estimateSize: Long = 0L
  
}
