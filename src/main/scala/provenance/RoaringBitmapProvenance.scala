package provenance

import org.roaringbitmap.RoaringBitmap

class RoaringBitmapProvenance(var bitmap: RoaringBitmap) extends Provenance {
  override def cloneProvenance(): Provenance = new RoaringBitmapProvenance(bitmap.clone())
  
  override def merge(other: Provenance): this.type = {
    other match {
      case rbp: RoaringBitmapProvenance =>
        bitmap.or(rbp.bitmap)
        //rbp.bitmap.clear()
      case other => throw new NotImplementedError(s"Unknown provenance type! $other")
    }
    this
  }
  
  /** Returns number of provenance IDs. */
  override def count: Int = bitmap.getCardinality
  
  /** Returns estimate in serialization size, experimental. */
  override def estimateSize: Long = bitmap.getLongSizeInBytes
  
  
}
