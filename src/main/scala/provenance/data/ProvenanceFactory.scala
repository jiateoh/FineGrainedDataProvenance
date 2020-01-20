package provenance.data

trait ProvenanceFactory {
  def create(id: Long): Provenance
}
