package trackers

import scala.reflect.ClassTag

abstract class BaseTracker[T:ClassTag](var value: T) extends Serializable {
  Trackers.count += 1
  // hashCode and equals are defined as proxies to underlying payload
  override final def hashCode : Int = value.hashCode
  
  // hashCode and equals are defined as proxies to underlying payload
  override final def equals(obj: scala.Any): Boolean = {
    obj match {
      case t: BaseTracker[T] => this.value.equals(t.value)
      case _ => value.equals(obj)
    }
  }
  
  override def toString: String = s"""Tracker[${value.toString}]"""
  
  /** Returns a new instance of the current tracker type with the provided value, but retaining
    * existing tracking (provenance) information. */
  def withValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U]
  
  /** Similar to #withValue, but creates a copy of the provenance information rather than reuse. */
  def cloneWithValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U]
  
  
  /** Merges the provided tracker provenance into the current instance. This method does not
    * consider anything about the value type of the other tracker. It may be assumed that the
    * secondary tracker's provenance will not be used after this operation.
    */
  def mergeTrackerProvenance(other: BaseTracker[_]): BaseTracker[T]
  
  /** Returns number of provenance IDs in the current tracker. */
  def provenanceCount: Int
  
  /** Returns estimate in serialization size, experimental. */
  def provenanceSizeEst: Long
}
