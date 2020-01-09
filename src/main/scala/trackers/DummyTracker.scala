package trackers
import scala.reflect.ClassTag

/** Dummy instance that stores nothing and acts only as a wrapper around value. This is intended
  * only for testing. */
class DummyTracker[T: ClassTag](value: T) extends BaseTracker[T](value) {
  override def withValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] =
    new DummyTracker(newValue)
  
  override def cloneWithValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] =
    new DummyTracker(newValue)
  
  override def mergeTrackerProvenance(other: BaseTracker[_]): BaseTracker[T] = this
  
  override def provenanceCount: Int = 0
  
  override def provenanceSizeEst: Long = 0L
}
