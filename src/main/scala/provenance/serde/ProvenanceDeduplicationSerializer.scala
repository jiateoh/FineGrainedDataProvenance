package provenance.serde

import java.io.{InputStream, OutputStream}
import java.nio.ByteBuffer

import org.apache.spark.Partitioner
import org.apache.spark.serializer.{DeserializationStream, SerializationStream, Serializer, SerializerInstance}
import provenance.data.{LazyCloneProvenance, Provenance}
import provenance.serde.ProvenanceDeduplicationSerializer.{PartitionProvenanceIdPair, PartitionProvenancePair, ProvenanceId, ProvenanceIdWrapper, SerProvenance}

import scala.collection.mutable
import scala.reflect.ClassTag

class ProvenanceDeduplicationSerializer(base: Serializer,
                                        partitioner: Partitioner)
  extends Serializer with Serializable {
  override def newInstance(): SerializerInstance = {
    new ProvenanceDeduplicationSerializerInstance(base.newInstance(), partitioner)
  }
  
}
object ProvenanceDeduplicationSerializer {
  private val debug: Boolean = false
  def printDebug(str: String) = {
    if(debug) println(str)
  }
  // unique identifier to determine if we can dedupe within a given key/partition.
  type KeyProvenancePair = (Any, Provenance)
  type PartitionProvenancePair = (Int, Provenance)
  // unique shorthand ID for a provenance object
  type ProvenanceId = Int
  type PartitionProvenanceIdPair = (Int, ProvenanceId)
  // serialized format, either 'deduped' or not
  type SerProvenance = Either[Provenance, ProvenanceId]
  implicit class ProvenanceIdWrapper(prov: Provenance) {
    def uniqueId(): Int = {
      prov.hashCode()
    }
  }
}

private class ProvenanceDeduplicationSerializerInstance(base: SerializerInstance,
                                                        partitioner: Partitioner)
  extends SerializerInstance {
  // some methods not implemented because they should never be used.
  override def serialize[T: ClassTag](t: T): ByteBuffer = ???
  
  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???
  
  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???
  
  override def serializeStream(s: OutputStream): SerializationStream = new
      ProvenanceDeduplicationSerializationStream(base.serializeStream(s), partitioner)
  
  override def deserializeStream(s: InputStream): DeserializationStream =
    new ProvenanceDeduplicationDeserializationStream(base.deserializeStream(s))
}

// Copied from JavaSerializationStream, without extra debugging info. Intention to customize this
// with enhancement for provenance-specific serialization.
// A few assumptions going on here...
// (1) writeKey is always called before writeValue.
// (2) writeValue will contain some sort of Provenance instances, which we can 'cache'.
private class ProvenanceDeduplicationSerializationStream(base: SerializationStream,
                                                         partitioner: Partitioner)
  extends SerializationStream {
  
  def writeObject[T: ClassTag](t: T): SerializationStream = base.writeObject(t)
  
  private var lastKey: Any = _
  override def writeKey[T: ClassTag](key: T): SerializationStream =
  {
    lastKey = key
    base.writeKey(key)
  }
  
  // In practice, we can limit the size of this map since the fallback option is just to write an
  // entire provenance map again (as opposed to the individual value).
  private val cache: mutable.Set[PartitionProvenanceIdPair] = mutable.Set()
  
  override def writeValue[T: ClassTag](provenanceRow: T): SerializationStream = {
    val (value, prov) = provenanceRow.asInstanceOf[(Any, Provenance)]
    val id = prov.uniqueId()
    val cacheKey = (partitioner.getPartition(lastKey), id)
    val serProv: SerProvenance = if(cache.contains(cacheKey)) {
      Right(id)
    } else {
      cache.add(cacheKey)
      Left(prov)
    }
    
    ProvenanceDeduplicationSerializer.printDebug(
      s"[${Thread.currentThread().getId}]SER: ($lastKey, ($value, $serProv)) " +
              s"[cacheKey=$cacheKey]")
    base.writeValue((value, serProv))
  }
  
  def flush() { base.flush() }
  def close() { base.close() }
}

// Copied from JavaSerializationStream, without extra debugging info. Intention to customize this
// with enhancement for provenance-specific serialization.
// A few assumptions going on here...
// (1) writeKey is always called before writeValue.
// (2) writeValue will contain some sort of Provenance instances, which we can 'cache'.
private class ProvenanceDeduplicationSerializationStreamOld(base: SerializationStream,
                                                         partitioner: Partitioner)
  extends SerializationStream {
  
  def writeObject[T: ClassTag](t: T): SerializationStream = base.writeObject(t)
  
  private var lastKey: Any = _
  override def writeKey[T: ClassTag](key: T): SerializationStream =
  {
    lastKey = key
    base.writeKey(key)
  }
  
  // In practice, we can limit the size of this map since the fallback option is just to write an
  // entire provenance map again (as opposed to the individual value).
  private val cache: mutable.HashMap[PartitionProvenancePair, ProvenanceId] = new mutable.HashMap()
  
  override def writeValue[T: ClassTag](provenanceRow: T): SerializationStream = {
    var (value, prov) = provenanceRow.asInstanceOf[(Any, Provenance)]
    // unwrap clones before writing them out
    prov = prov match {
      case clone: LazyCloneProvenance => clone.orig
      case _ => prov
    }
    val cacheKey = (partitioner.getPartition(lastKey), prov)
    val serProv: SerProvenance = if(cache.contains(cacheKey)) {
      Right(cache(cacheKey))
    } else {
      cache.put(cacheKey, prov.uniqueId())
      Left(prov)
    }
  
    ProvenanceDeduplicationSerializer.printDebug(s"[${Thread.currentThread().getId}]SER: $cacheKey: ($lastKey, ($value, $serProv))")
    base.writeValue((value, serProv))
  }
  
  def flush() { base.flush() }
  def close() { base.close() }
}

private class ProvenanceDeduplicationDeserializationStream(base: DeserializationStream)
  extends DeserializationStream {
  
  // Not necessary except for debugging
  private var lastKey: Any = _
  override def readKey[T: ClassTag](): T = {
    val result = base.readKey[T]()
    lastKey = result
    result
  }
  
  // In practice we might want to evict values if they are old or no longer needed, but we have
  // to be careful because premature eviction can lead to an error/loss of provenance.
  private val cache: mutable.HashMap[ProvenanceId, Provenance] = new mutable.HashMap()
  override def readValue[T: ClassTag](): T = {
    val (value, serProv) = base.readValue[(Any, SerProvenance)]
    val prov = serProv match {
      case Left(prov) =>
        ProvenanceDeduplicationSerializer.printDebug(s"[${Thread.currentThread().getId}]DESER: ($lastKey, ($value, $serProv)) ")
        cache.put(prov.uniqueId(), prov)
        prov
      case Right(id) =>
        val temp = cache(id)
        ProvenanceDeduplicationSerializer.printDebug(s"[${Thread.currentThread().getId}]DESER: ($lastKey, ($value, $serProv)) " +
                  s"(lookup: $temp)")
        temp
    }
    (value, prov).asInstanceOf[T]
  }
  
  def readObject[T: ClassTag](): T = base.readObject()
  def close(): Unit = base.close()
}