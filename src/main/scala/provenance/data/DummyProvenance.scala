package provenance.data

class DummyProvenance extends Provenance {
  override def cloneProvenance(): Provenance = this
  
  override def merge(other: Provenance): Provenance = other
  
  override def count: Int = 0
  
  override def estimateSize: Long = 0L
  
}


object DummyProvenance extends ProvenanceFactory {
  override def create(id: Long): Provenance = new DummyProvenance()
}