package provenance

import provenance.data.Provenance

package object rdd {
  type ProvenanceRow[T] = (T, Provenance)
}
