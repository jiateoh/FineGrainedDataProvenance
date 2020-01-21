package trackers

import org.roaringbitmap.RoaringBitmap
import provenance.data.{DummyProvenance, RoaringBitmapProvenance}

import scala.collection.mutable
import scala.reflect.ClassTag

/** Global access object for managing new tracker RDDs. This should be modifieid to be
  * configurable in the future via the confs rather than a separate method. */
object Trackers {
  var count = 0
  private var trackerCreator: TrackerCreator =
    //RoaringBitMapTrackerCreator
    //SetTrackerCreator
    DummyTrackerCreator
  
  
  def createTracker[T](value: T, id: Long)
                      (implicit ct: ClassTag[T]): BaseTracker[T] = {
      count += 1 // disable this to differentiate between RDD initialization and map/filter/etc
      trackerCreator.createTracker(value, id)(ct)
  }
  
  def setTrackerCreator(trackerCreator: TrackerCreator): Unit = {
    println("-" * 40)
    println(s"Provenance tracker set to ${trackerCreator.getClass.getSimpleName}")
    println("-" * 40)
    this.trackerCreator = trackerCreator
  }
  
  def setTrackerCreator(trackerCreatorStr: String): Unit = {
    trackerCreatorStr.toLowerCase() match {
      case "bitmap" | "roaring" | "roaringbitmap" | "rr" =>
        setTrackerCreator(RoaringBitMapTrackerCreator)
      case "bitmapprov" =>
        setTrackerCreator(RoaringBitMapProvenanceTrackerCreator)
      case "set" | "naive" =>
        setTrackerCreator(SetTrackerCreator)
      case "dummy" =>
        setTrackerCreator(DummyTrackerCreator)
      case "dummyprov" =>
        setTrackerCreator(DummyProvenanceTrackerCreator)
      case "test" | "testing" =>
        setTrackerCreator(TestingTrackerCreator)
      case _ =>
        throw new UnsupportedOperationException(s"Unknown tracker type: ${trackerCreatorStr}")
    }
  }
  
  /** Debugging print statement wrapped in try-catch, used to keep separators close to final
    * output, e.g. to avoid confusion with Spark logs which can be a bit excessive.
    */
  def printDebug(block: => Object, onError: (Throwable) => String, sep: String = "-" * 50) : Unit = {
    val temp: String = try {
      block.toString
    } catch {
      case t: Throwable => onError(t)
    }
    println(sep)
    println(temp)
    println(sep)
  }
  
  def printDebug(block: => Object, errorStr: String) : Unit = {
    printDebug(block, (_: Throwable) => errorStr)
  }
}

trait TrackerCreator {
  def createTracker[T](value: T, id: Long)
                      (implicit ct: ClassTag[T]): BaseTracker[T]
}

object RoaringBitMapTrackerCreator extends TrackerCreator {
  def createTracker[T](value: T, id: Long)
                                   (implicit ct: ClassTag[T]): BaseTracker[T]  = {
    val rr = new RoaringBitmap
    if (id > Int.MaxValue)
    // jteoh: Roaring64NavigableBitmap should be an option if this is required.
      throw new UnsupportedOperationException(
        "The offset is greater than Int.Max which is not supported yet")
    rr.add(id.asInstanceOf[Int])
    new RoaringBitmapTracker(value, rr)
  }
}

object RoaringBitMapProvenanceTrackerCreator extends TrackerCreator {
  def createTracker[T](value: T, id: Long)
                      (implicit ct: ClassTag[T]): BaseTracker[T]  = {
    val rr = new RoaringBitmap
    if (id > Int.MaxValue)
    // jteoh: Roaring64NavigableBitmap should be an option if this is required.
      throw new UnsupportedOperationException(
        "The offset is greater than Int.Max which is not supported yet")
    rr.add(id.asInstanceOf[Int])
    //new RoaringBitmapTracker(value, rr)
    new ProvenanceTracker(value, new RoaringBitmapProvenance(rr))
  }
}

object SetTrackerCreator extends TrackerCreator {
  def createTracker[T](value: T, id: Long)
                         (implicit ct: ClassTag[T]): SetTracker[T] = {
    if(id > Int.MaxValue)
    // We could create a Set[Long] structure, but that would double the memory overhead so we
    // stick to int until necessary.
      throw new UnsupportedOperationException(
        "The offset is greater than Int.Max which is not supported yet")
    def provenance = mutable.Set(id.asInstanceOf[Int])
    new SetTracker[T](value, provenance)
  }
}

object TestingTrackerCreator extends TrackerCreator {
  def createTracker[T](value: T, id: Long)
                      (implicit ct: ClassTag[T]): TestingTracker[T] = {
    if(id > Int.MaxValue)
    // We could create a Set[Long] structure, but that would double the memory overhead so we
    // stick to int until necessary.
      throw new UnsupportedOperationException(
        "The offset is greater than Int.Max which is not supported yet")
    def provenance = mutable.Set(id.asInstanceOf[Int])
    new TestingTracker[T](value, provenance)
  }
}

object DummyTrackerCreator extends TrackerCreator {
  override def createTracker[T](value: T, id: Long)
                               (implicit ct: ClassTag[T]): BaseTracker[T] =
    new DummyTracker[T](value)
}

object DummyProvenanceTrackerCreator extends TrackerCreator {
  override def createTracker[T](value: T, id: Long)
                               (implicit ct: ClassTag[T]): BaseTracker[T] =
  //new DummyTracker[T](value)
    new ProvenanceTracker(value, new DummyProvenance())
}