package symbolicprimitives

import org.roaringbitmap.RoaringBitmap

object StringImplicits {
  implicit def symString2string(s: SymString): String = s.getValue()

  /*implicit class SymStringImplic(s:String){
    def toSymString = {
      new SymString(s)
    }
  }
}*/
}
/**
  * Created by malig on 4/29/19.
  */
 class SymString(i: String, rr: RoaringBitmap)
    extends Serializable
{

  private val value: String = i

  def this(i:String, influenceOffset : Long) {
    this(i,new RoaringBitmap)

    if(influenceOffset > Int.MaxValue) {
      throw new UnsupportedOperationException("The offset is greater than Int.Max which is not supported yet")
    }

    this.rr.add(influenceOffset.asInstanceOf[Int])
  }

  // TODO: Implement the influence/rank function here
  def mergeProvenance(rr_other : RoaringBitmap): RoaringBitmap = {
    RoaringBitmap.or(rr, rr_other)
  }

  def getProvenanceSize(): Int ={
    rr.getSizeInBytes
  }

  def getProvenance(): RoaringBitmap ={
    rr
  }

  def getValue(): String = {
    return value
  }

  override def toString: String =
    value.toString + s""" (Provenance Bitmap: ${rr})"""

  /**
    * Unsupported Operations
    */

   def length: SymInt = {
     new SymInt( value.length, rr)
  }

   def split(separator: Char): Array[SymString] = {
    value
      .split(separator)
      .map(s =>
        new SymString(
          s, rr))
  }

   def split(separator: Array[Char]): Array[SymString] = {

    value
      .split(separator)
      .map(s =>
        new SymString(
          s, rr
        ))
  }

   def toInt: SymInt ={
    new SymInt( value.toInt, rr)
  }

   def toFloat: SymFloat =
     new SymFloat(value.toFloat , rr)

   def toDouble: SymDouble ={
    new SymDouble(value.toDouble , rr)
  }

  override def hashCode : Int = value.hashCode

  override def equals(obj: scala.Any): Boolean = {
  obj match {
    case str: SymString => this.equals(str)
    case _ =>  value.equals(obj)
  }

  }
  def equals(obj: SymString): Boolean = value.equals(obj.getValue())
  def eq(obj: SymString): Boolean = value.eq(obj.getValue())

}
