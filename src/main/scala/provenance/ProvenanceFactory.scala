package provenance

trait ProvenanceFactory {
  def create(id: Long): Provenance
}