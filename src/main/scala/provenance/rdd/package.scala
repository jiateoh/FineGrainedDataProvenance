package provenance

package object rdd {
  type ProvenanceRow[T] = (T, Provenance)
}
