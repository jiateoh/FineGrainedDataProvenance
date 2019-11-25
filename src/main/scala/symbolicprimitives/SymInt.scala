package symbolicprimitives

/**
  * Created by malig on 4/25/19.
  */

import org.roaringbitmap.RoaringBitmap

object SymImplicits {

  implicit def symInt2Int(s: SymInt): Int = s.getValue()
  implicit def symFloat2Float(s: SymFloat): Float = s.getValue()
  implicit def symDouble2Double(s: SymDouble): Double = s.getValue()

  implicit def int2SymInt(s: Int): SymInt = new SymInt(s, new RoaringBitmap())
  implicit def float2SymFloat(s: Float): SymFloat = new SymFloat(s, new RoaringBitmap())
  implicit def double2SymDouble(s: Double): SymDouble = new SymDouble(s, new RoaringBitmap())

  implicit def symInt2String(s: SymInt): String = s.getValue().toString
  implicit def symFloat2String(s: SymFloat): String = s.getValue().toString
  implicit def symDouble2String(s: SymDouble): String = s.getValue().toString

  implicit def symInt2SymFloat(s: SymInt): SymFloat = new SymFloat(s.getValue() , s.getProvenance())
  implicit def symFloat2SymInt(s: SymFloat): SymInt = new SymInt(s.getValue().toInt , s.getProvenance())
  implicit def symFloat2SymDouble(s: SymFloat): String = new SymDouble(s.getValue().toDouble , s.getProvenance())
  implicit def symInt2SymDouble(s: SymInt): String = new SymDouble(s.getValue().toDouble , s.getProvenance())
  // Todo: Insert s.getClauses

}

class SymInt(i: Int, rr : RoaringBitmap) extends Serializable{

  private val value: Int = i

  def this(i:Int, influenceOffset : Long) {
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

  def getValue(): Int = {
    return value
  }

  /**
    * Overloading operators from here onwards
    */

  override def hashCode(): Int = value.hashCode()
  override def equals(obj: scala.Any): Boolean =
  obj match {
      case x: SymInt => value.equals(x.getValue())
      case x: SymDouble => value.equals(x.getValue())
      case x: SymFloat => value.equals(x.getValue())
      case _ =>  value.equals(obj)
    }

  def +(x: Int): SymInt = {
    val d = value + x
    new SymInt(d, rr)
  }

  def -(x: Int): SymInt = {
    val d = value - x
    new SymInt(d, rr)
  }

  def *(x: Int): SymInt = {
    val d = value * x
    new SymInt(d, rr)
  }

  def *(x: Float): SymFloat = {
    val d = value * x
    new SymFloat(d, rr)
  }


  def /(x: Int): SymDouble= {
    val d = value / x
    new SymDouble(d, rr )
  }

  def /(x: Long): SymDouble= {
    val d = value / x
    new SymDouble(d, rr)
  }

  def +(x: SymInt): SymInt = {
    new SymInt(value + x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def -(x: SymInt): SymInt = {
    new SymInt(value - x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def *(x: SymInt): SymInt = {
    new SymInt(value * x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def /(x: SymInt): SymInt = {
    new SymInt(value / x.getValue(), mergeProvenance(x.getProvenance()))
  }

  /**
    * Operators not supported yet
    */

  def ==(x: Int): Boolean = value == x

  override def toString: String =
    value.toString + s""" (Provenance Bitmap: ${rr})"""

  def toByte: Byte = value.toByte

  def toShort: Short = value.toShort

  def toChar: Char = value.toChar

  def toInt: Int = value.toInt

  def toLong: Long = value.toLong

  def toFloat: Float = value.toFloat

  def toDouble: Double = value.toDouble

  def unary_~ : Int = value.unary_~

  def unary_+ : Int = value.unary_+

  def unary_- : Int = value.unary_-

  def +(x: String): String = value + x

  def <<(x: Int): Int = value << x

  def <<(x: Long): Int = value << x

  def >>>(x: Int): Int = value >>> x

  def >>>(x: Long): Int = value >>> x

  def >>(x: Int): Int = value >> x

  def >>(x: Long): Int = value >> x

  def ==(x: Byte): Boolean = value == x

  def ==(x: Short): Boolean = value == x

  def ==(x: Char): Boolean = value == x

  def ==(x: Long): Boolean = value == x

  def ==(x: Float): Boolean = value == x

  def ==(x: Double): Boolean = value == x

  def !=(x: Byte): Boolean = value != x

  def !=(x: Short): Boolean = value != x

  def !=(x: Char): Boolean = value != x

  def !=(x: Int): Boolean = value != x

  def !=(x: Long): Boolean = value != x

  def !=(x: Float): Boolean = value != x

  def !=(x: Double): Boolean = value != x

  def <(x: Byte): Boolean = value < x

  def <(x: Short): Boolean = value < x

  def <(x: Char): Boolean = value < x

  def <(x: Int): Boolean = value < x

  def <(x: Long): Boolean = value < x

  def <(x: Float): Boolean = value < x

  def <(x: Double): Boolean = value < x

  def <=(x: Byte): Boolean = value <= x

  def <=(x: Short): Boolean = value <= x

  def <=(x: Char): Boolean = value <= x

  def <=(x: Int): Boolean = value <= x

  def <=(x: Long): Boolean = value <= x

  def <=(x: Float): Boolean = value <= x

  def <=(x: Double): Boolean = value <= x

  def >(x: Byte): Boolean = value > x

  def >(x: Short): Boolean = value > x

  def >(x: Char): Boolean = value > x

  def >(x: Int): Boolean = value > x

  def >(x: Long): Boolean = value > x

  def >(x: Float): Boolean = value > x

  def >(x: Double): Boolean = value > x

  def >=(x: Byte): Boolean = value >= x

  def >=(x: Short): Boolean = value >= x

  def >=(x: Char): Boolean = value >= x

  def >=(x: Int): Boolean = value >= x

  def >=(x: Long): Boolean = value >= x

  def >=(x: Float): Boolean = value >= x

  def >=(x: Double): Boolean = value >= x

  def |(x: Byte): Int = value | x

  def |(x: Short): Int = value | x

  def |(x: Char): Int = value | x

  def |(x: Int): Int = value | x

  def |(x: Long): Long = value | x

  def &(x: Byte): Int = value & x

  def &(x: Short): Int = value & x

  def &(x: Char): Int = value & x

  def &(x: Int): Int = value & x

  def &(x: Long): Long = value & x

  def ^(x: Byte): Int = value ^ x

  def ^(x: Short): Int = value ^ x

  def ^(x: Char): Int = value ^ x

  def ^(x: Int): Int = value ^ x

  def ^(x: Long): Long = value ^ x

  def +(x: Byte): Int = value + x

  def +(x: Short): Int = value + x

  def +(x: Char): Int = value + x

  def +(x: Long): Long = value + x

  def +(x: Float): Float = value + x

  def +(x: Double): Double = value + x

  def -(x: Byte): Int = value - x

  def -(x: Short): Int = value - x

  def -(x: Char): Int = value - x

  def -(x: Long): Long = value - x

  def -(x: Float): Float = value - x

  def -(x: Double): Double = value - x

  def *(x: Byte): Int = value * x

  def *(x: Short): Int = value * x

  def *(x: Char): Int = value * x

  def *(x: Long): Long = value * x

  def *(x: Double): Double = value * x

  def /(x: Byte): Int = value / x

  def /(x: Short): Int = value / x

  def /(x: Char): Int = value / x

  def /(x: Float): Float = value / x

  def /(x: Double): Double = value / x

  def %(x: Byte): Int = value % x

  def %(x: Short): Int = value % x

  def %(x: Char): Int = value % x

  def %(x: Int): Int = value % x

  def %(x: Long): Long = value % x

  def %(x: Float): Float = value % x

  def %(x: Double): Double = value % x

}