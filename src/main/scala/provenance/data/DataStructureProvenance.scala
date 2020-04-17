package provenance.data

abstract class DataStructureProvenance(private var data: Any) extends Provenance {
  /*override def hashCode(): Int = data.hashCode()
  
  override def equals(obj: Any): Boolean = {
    obj match {
      case other: DataStructureProvenance =>
        data.equals(other.data)
      case clone: LazyCloneProvenance =>
        this.equals(clone.orig)
      case _ => false
    }
  }

  */
  override def equals(other: Any): Boolean = {
      true
  }

  override def hashCode(): Int = {
      0
  }
}
