package provenance

import org.apache.spark.SparkContext

trait Provenance extends Serializable {
  
  // Renamed because clone() is a built-in function.
  def cloneProvenance(): Provenance
  
  /** Merges two provenance instances (in place), returning the current instance after merging. */
  def merge(other: Provenance): this.type 
  
  /** Returns number of provenance IDs. */
  def count: Int
  
  /** Returns estimate in serialization size, experimental. */
  def estimateSize: Long
  
}

object Provenance {
  var count = 0
  private var provenanceFactory: ProvenanceFactory = RoaringBitmapProvenance
  private var initialized: Boolean = false
  
  def createProvenance(id: Long): Provenance = {
    //if(!initialized) initializeFromConf()
    count += 1
    provenanceFactory.create(id)
  }
  
  def setProvenance(provenanceFactory: ProvenanceFactory): Unit = {
    initialized = true
    println("-" * 40)
    println(s"Provenance tracker set to ${provenanceFactory.getClass.getSimpleName}")
    println("-" * 40)
    this.provenanceFactory = provenanceFactory
  }
  
  def setProvenance(provenanceFactoryStr: String): Unit = {
    val newFactory = provenanceFactoryStr match {
      case "dummy" => DummyProvenance
      case "bitmap" => RoaringBitmapProvenance
      case _ => throw new UnsupportedOperationException(s"Unknown provenance type: $provenanceFactoryStr")
    }
    setProvenance(newFactory)
  }
  
  
  /** Debugging print statement wrapped in try-catch, used to keep separators close to final
    * output, e.g. to avoid confusion with Spark logs which can be a bit excessive.
    */
  def printDebug(block: => Object, onError: (Throwable) => String, sep: String = "-" * 50): Unit = {
    val temp: String = try {
      block.toString
    } catch {
      case t: Throwable => onError(t)
    }
    println(sep)
    println(temp)
    println(sep)
  }
  
  def printDebug(block: => Object, errorStr: String): Unit = {
    printDebug(block, (_: Throwable) => errorStr)
  }
}