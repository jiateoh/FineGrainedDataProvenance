package provenance

import provenance.data.Provenance
import symbolicprimitives.SymBase

package object rdd {
  type ProvenanceRow[T] = (T, Provenance)
  type CombinerWithInfluence[T,V] = (T, V)

}
