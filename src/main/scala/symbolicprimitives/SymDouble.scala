package symbolicprimitives

import provenance.data.Provenance

/**
  * Created by malig on 4/25/19.
  */
case class SymDouble(i: Double, p: Provenance) extends SymBase(p) {

  private val value: Double = i

  // TODO: Implement the influence/rank function here
  def mergeProvenance(prov_other: Provenance): Provenance = {
    prov.merge(prov_other)
  }
  
  def getValue(): Double = {
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

  def +(x: Double): SymDouble = {
    new SymDouble(value + x, prov)
  }

  def -(x: Double): SymDouble = {
    new SymDouble(value - x, prov)
  }

  def *(x: Double): SymDouble = {
    new SymDouble(value * x, prov)

  }

  def *(x: Float): SymDouble = {
    new SymDouble(value * x, prov)
  }

  def /(x: Double): SymDouble = {
    new SymDouble(value / x, prov)
  }

  def +(x: SymDouble): SymDouble = {
    new SymDouble(value + x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def -(x: SymDouble): SymDouble = {
    new SymDouble(value - x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def *(x: SymDouble): SymDouble = {
    new SymDouble(value * x.getValue(), mergeProvenance(x.getProvenance()))
  }

  def /(x: SymDouble): SymDouble = {
    new SymDouble(value / x.getValue(), mergeProvenance(x.getProvenance()))
  }

  // TODO: Following are control flow provenance that, in my opinion, should be configurable. [Gulzar]

  def <(x: SymDouble): Boolean = {
    prov.merge(x.getProvenance())
    return value < x.getValue()
  }

  def <(x: Double): Boolean = {
    return value < x
  }

  def >=(x: SymDouble): Boolean = {
    prov.merge(x.getProvenance())
    value >= x.getValue()
  }
  def <=(x: SymDouble): Boolean = {
    prov.merge(x.getProvenance())
    value <= x.getValue()
  }

  def ==(x: Int): Boolean = {
    value == x
  }
  def >(x: Int): Boolean = {
    value > x
  }

  override def toString: String =
    value.toString + s""" (Most Influential Input Offset: ${prov})"""
  /**
    * Not Supported Symbolically yet
    **/
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
  //
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
