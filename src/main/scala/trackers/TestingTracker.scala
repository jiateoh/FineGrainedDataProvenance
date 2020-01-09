package trackers

import org.apache.spark.util.SizeEstimator

import scala.collection.mutable
import scala.reflect.ClassTag

// currently same as set, but does not clone.
class TestingTracker [T: ClassTag](value: T, var provenance: mutable.Set[Int]) extends BaseTracker[T](value) {
  
  override def toString: String = s""" ${value.toString} --> Size: ${provenance.size} """
  
  override def withValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] =
    new SetTracker(newValue, this.provenance)
  
  override def cloneWithValue[U](newValue: U)(implicit ct: ClassTag[U]): BaseTracker[U] =
    new SetTracker(newValue, this.provenance)
  
  override def mergeTrackerProvenance(other: BaseTracker[_]): BaseTracker[T] =
    other match {
      case st: SetTracker[_] =>
        this.provenance ++= st.provenance
        st.provenance.clear()
        this
      case _ => throw new NotImplementedError(s"Unknown tracker type! $other")
    }
  
  override def provenanceCount: Int = provenance.size
  
  override def provenanceSizeEst: Long = SizeEstimator.estimate(provenance)
  
}

