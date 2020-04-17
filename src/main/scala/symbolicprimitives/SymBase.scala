package symbolicprimitives

import provenance.data.Provenance

abstract class SymBase(var prov: Provenance) extends Serializable{

  def getProvenance() : Provenance = prov
  def setProvenance(p : Provenance) = prov = p
}
