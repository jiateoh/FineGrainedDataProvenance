package symbolicprimitives

import org.roaringbitmap.RoaringBitmap

/**
  * Created by malig on 4/25/19.
  */

class SymFloat(i: Float, rr:RoaringBitmap) extends Serializable{

  private var value: Float = i

  def this(i:Float, influenceOffset : Long) {
    this(i,new RoaringBitmap)

    if(influenceOffset > Int.MaxValue) {
      throw new UnsupportedOperationException("The offset is greater than Int.Max which is not supported yet")
    }

    rr.add(influenceOffset.asInstanceOf[Int])
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

  def getValue(): Float = {
    return value
  }

  /**
    * Overloading operators from here onwards
    */

  override def hashCode(): Int = value.hashCode()
  override def equals(obj: scala.Any): Boolean =  obj match {
    case x: SymInt => value.equals(x.getValue())
    case x: SymDouble => value.equals(x.getValue())
    case x: SymFloat => value.equals(x.getValue())
    case _ =>  value.equals(obj)
  }

  def +(x: Float): SymFloat = {
    val d = value + x
    new SymFloat(d, rr)
  }

  def -(x: Float): SymFloat = {
    val d = value - x
    new SymFloat(d, rr)
  }

  def *(x: Float): SymFloat = {
    val d = value * x
    new SymFloat(d, rr)

  }

  def /(x: Float): SymFloat = {
    val d = value / x
    new SymFloat(d, rr)
  }

  def +(x: SymFloat): SymFloat = {
    new SymFloat(value + x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def +(x: SymDouble): SymDouble = {
    new SymDouble(value + x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def -(x: SymFloat): SymFloat = {
    new SymFloat(value - x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def *(x: SymFloat): SymFloat = {
    new SymFloat(value * x.getValue(), mergeProvenance(x.getProvenance()))

  }

  def /(x: SymFloat): SymFloat = {
    new SymFloat(value / x.getValue(), mergeProvenance(x.getProvenance()))
  }

  /**
    * Operators not Supported Symbolically yet
    **/
  override def toString: String =
    value.toString + s""" (Most Influential Input Offset: ${rr})"""
//
//  def toByte: Byte = value.toByte
//
//  def toShort: Short = value.toShort
//
//  def toChar: Char = value.toChar
//
//  def toInt: Int = value.toInt
//
//  def toLong: Long = value.toLong
//
//  def toFloat: Float = value.toFloat
//
//  def toDouble: Double = value.toDouble
//
//  def unary_~ : Int = value.unary_~
//
//  def unary_+ : Int = value.unary_+
//
//  def unary_- : Int = value.unary_-
//
//  def +(x: String): String = value + x
//
//  def <<(x: Int): Int = value << x
//
//  def <<(x: Long): Int = value << x
//
//  def >>>(x: Int): Int = value >>> x
//
//  def >>>(x: Long): Int = value >>> x
//
//  def >>(x: Int): Int = value >> x
//
//  def >>(x: Long): Int = value >> x
//
//  def ==(x: Byte): Boolean = value == x
//
//  def ==(x: Short): Boolean = value == x
//
//  def ==(x: Char): Boolean = value == x
//
//  def ==(x: Long): Boolean = value == x
//
//  def ==(x: Float): Boolean = value == x
//
//  def ==(x: Double): Boolean = value == x
//
//  def !=(x: Byte): Boolean = value != x
//
//  def !=(x: Short): Boolean = value != x
//
//  def !=(x: Char): Boolean = value != x
//
//  def !=(x: Int): Boolean = value != x
//
//  def !=(x: Long): Boolean = value != x
//
//  def !=(x: Float): Boolean = value != x
//
//  def !=(x: Double): Boolean = value != x
//
//  def <(x: Byte): Boolean = value < x
//
//  def <(x: Short): Boolean = value < x
//
//  def <(x: Char): Boolean = value < x
//
//  def <(x: Int): Boolean = value < x
//
//  def <(x: Long): Boolean = value < x
//
//  def <(x: Float): Boolean = value < x
//
//  def <(x: Double): Boolean = value < x
//
//  def <=(x: Byte): Boolean = value <= x
//
//  def <=(x: Short): Boolean = value <= x
//
//  def <=(x: Char): Boolean = value <= x
//
//  def <=(x: Int): Boolean = value <= x
//
//  def <=(x: Long): Boolean = value <= x
//
//  def <=(x: Float): Boolean = value <= x
//
//  def <=(x: Double): Boolean = value <= x
//
//  def >(x: Byte): Boolean = value > x
//
//  def >(x: Short): Boolean = value > x
//
//  def >(x: Char): Boolean = value > x
//
//  def >(x: Int): Boolean = value > x
//
//  def >(x: Long): Boolean = value > x
//
//  def >(x: Float): Boolean = value > x
//
//  def >(x: Double): Boolean = value > x
//
//  def >=(x: Byte): Boolean = value >= x
//
//  def >=(x: Short): Boolean = value >= x
//
//  def >=(x: Char): Boolean = value >= x
//
//  def >=(x: Int): Boolean = value >= x
//
//  def >=(x: Long): Boolean = value >= x
//
//  def >=(x: Float): Boolean = value >= x
//
//  def >=(x: Double): Boolean = value >= x
//
//  def |(x: Byte): Int = value | x
//
//  def |(x: Short): Int = value | x
//
//  def |(x: Char): Int = value | x
//
//  def |(x: Int): Int = value | x
//
//  def |(x: Long): Long = value | x
//
//  def &(x: Byte): Int = value & x
//
//  def &(x: Short): Int = value & x
//
//  def &(x: Char): Int = value & x
//
//  def &(x: Int): Int = value & x
//
//  def &(x: Long): Long = value & x
//
//  def ^(x: Byte): Int = value ^ x
//
//  def ^(x: Short): Int = value ^ x
//
//  def ^(x: Char): Int = value ^ x
//
//  def ^(x: Int): Int = value ^ x
//
//  def ^(x: Long): Long = value ^ x
//
//  def +(x: Byte): Int = value + x
//
//  def +(x: Short): Int = value + x
//
//  def +(x: Char): Int = value + x
//
//  def +(x: Long): Long = value + x
//
//  def +(x: Float): Float = value + x
//
//  def +(x: Double): Double = value + x
//
//  def -(x: Byte): Int = value - x
//
//  def -(x: Short): Int = value - x
//
//  def -(x: Char): Int = value - x
//
//  def -(x: Long): Long = value - x
//
//  def -(x: Float): Float = value - x
//
//  def -(x: Double): Double = value - x
//
//  def *(x: Byte): Int = value * x
//
//  def *(x: Short): Int = value * x
//
//  def *(x: Char): Int = value * x
//
//  def *(x: Long): Long = value * x
//
//  def *(x: Float): Float = value * x
//
//  def *(x: Double): Double = value * x
//
//  def /(x: Byte): Int = value / x
//
//  def /(x: Short): Int = value / x
//
//  def /(x: Char): Int = value / x
//
//  def /(x: Long): Long = value / x
//
//  def /(x: Float): Float = value / x
//
//  def /(x: Double): Double = value / x
//
//  def %(x: Byte): Int = value % x
//
//  def %(x: Short): Int = value % x
//
//  def %(x: Char): Int = value % x
//
//  def %(x: Int): Int = value % x
//
//  def %(x: Long): Long = value % x
//
//  def %(x: Float): Float = value % x
//
//  def %(x: Double): Double = value % x

}