package provenance.data

trait Provenance extends Serializable {
  
  def cloneProvenance(): Provenance
  
  /** Merges two provenance instances (in place), returning the current instance after merging. */
  def merge(other: Provenance): Provenance
  
  /** Returns number of provenance IDs. */
  def count: Int
  
  /** Returns estimate in serialization size, experimental. */
  def estimateSize: Long
  
}

object Provenance {
  var useLazyClone: Boolean = _
  def setLazyClone(lazyClone: Boolean): Unit = {
    println("-" * 40)
    println(s"Lazy clone configuration: $lazyClone")
    println("-" * 40)
    this.useLazyClone = lazyClone
  }
  setLazyClone(true)

  var useDedupSerializer: Boolean = _
  def setDedupSerializer(dedup: Boolean): Unit = {
    println("-" * 40)
    println(s"Deduplication serializer configuration: $dedup")
    println("-" * 40)
    this.useDedupSerializer = dedup
  }
  setDedupSerializer(true)
  
  var provenanceFactory: ProvenanceFactory = _
  setProvenanceFactory(RoaringBitmapProvenance)
  //setProvenanceFactory(SetProvenance)
  
  def setProvenanceFactory(provenanceFactory: ProvenanceFactory): Unit = {
    println("-" * 40)
    println(s"Provenance tracker set to ${provenanceFactory.getClass.getSimpleName}")
    println("-" * 40)
    this.provenanceFactory = provenanceFactory
  }
  
  def setProvenanceType(provenanceFactoryStr: String): Unit = {
    val newFactory = provenanceFactoryStr match {
      case "dummy" => DummyProvenance
      case "bitmap" => RoaringBitmapProvenance
      case "set" => SetProvenance
      case _ => throw new UnsupportedOperationException(s"Unknown provenance type: $provenanceFactoryStr")
    }
    setProvenanceFactory(newFactory)
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