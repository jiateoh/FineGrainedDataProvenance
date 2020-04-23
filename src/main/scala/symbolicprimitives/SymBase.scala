package symbolicprimitives

import provenance.data.Provenance

abstract class SymBase(var prov: Provenance) extends Serializable{

  def getProvenance() : Provenance = prov
  def setProvenance(p : Provenance) = prov = p
  
  // TODO: Implement the influence/rank function here
  def mergeProvenance(otherProv : Provenance): Provenance = {
    // TODO: disable cloning for now, but this is dangerous for correctness in something such as
    //  flatMap
    //prov.cloneProvenance().merge(prov_other)
    prov.merge(otherProv)
  }
  
}
