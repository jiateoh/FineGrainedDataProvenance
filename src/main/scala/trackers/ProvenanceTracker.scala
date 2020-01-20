package trackers

import provenance.data.Provenance

import scala.reflect.ClassTag

class ProvenanceTracker[T: ClassTag](value: T, var prov: Provenance) extends BaseTracker(value) {
  override def withValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] =
    new ProvenanceTracker[U](newValue, prov)
  
  override def cloneWithValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] =
    new ProvenanceTracker[U](newValue, prov.cloneProvenance())
  
  override def mergeTrackerProvenance(other: BaseTracker[_]): BaseTracker[T] = {
    other match {
      case bpt: ProvenanceTracker[_] => this.prov.merge(bpt.prov)
      case _ => throw new NotImplementedError(s"Unknown tracker type! $other")
    }
    this
  }
  
  override def provenanceCount: Int = prov.count
  
  override def provenanceSizeEst: Long = prov.estimateSize
}
