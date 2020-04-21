package symbolicprimitives

/**
  * Created by malig on 4/25/19.
  */

import provenance.data.Provenance

object SymImplicits {

  //TODO: Using zero as default provenance here. We need to chain this disconnect through dependency analysis

  implicit def int2SymInt(s: Int): SymInt = new SymInt(s, Provenance.provenanceFactory.create(-1))
  implicit def float2SymFloat(s: Float): SymFloat = new SymFloat(s, Provenance.provenanceFactory.create(-1))
  implicit def double2SymDouble(s: Double): SymDouble = new SymDouble(s, Provenance.provenanceFactory.create(-1))

  implicit def symInt2String(s: SymInt): String = s.getValue().toString
  implicit def symFloat2String(s: SymFloat): String = s.getValue().toString
  implicit def symDouble2String(s: SymDouble): String = s.getValue().toString

  implicit def symInt2SymFloat(s: SymInt): SymFloat = new SymFloat(s.getValue() , s.getProvenance())
  implicit def symFloat2SymInt(s: SymFloat): SymInt = new SymInt(s.getValue().toInt , s.getProvenance())
  implicit def symFloat2SymDouble(s: SymFloat): SymDouble = new SymDouble(s.getValue().toDouble , s.getProvenance())
  implicit def symInt2SymDouble(s: SymInt): SymDouble = new SymDouble(s.getValue().toDouble , s.getProvenance())

  // A few common tuple options - these implicitly rely on the conversions defined above.
  type SymLong = SymInt //TODO we don't have a SymLong type yet, so for simplicity we use SymInt. This is *not* accurate.
  implicit def long2SymLong(s: Long): SymLong = new SymInt(s.toInt, Provenance.provenanceFactory.create(-1))
  
  // There are 16 definitions for pairs: 4 x 4.
  implicit def intIntTupleToSyms(tuple: (Int, Int)): (SymInt, SymInt) = (tuple._1, tuple._2)
  implicit def intLongTupleToSyms(tuple: (Int, Long)): (SymInt, SymLong) = (tuple._1, tuple._2)
  implicit def intDoubleTupleToSyms(tuple: (Int, Double)): (SymInt, SymDouble) = (tuple._1, tuple._2)
  implicit def intFloatTupleToSyms(tuple: (Int, Float)): (SymInt, SymFloat) = (tuple._1, tuple._2)
  
  implicit def longIntTupleToSyms(tuple: (Long, Int)): (SymLong, SymInt) = (tuple._1, tuple._2)
  implicit def longLongTupleToSyms(tuple: (Long, Long)): (SymLong, SymLong) = (tuple._1, tuple._2)
  implicit def longDoubleTupleToSyms(tuple: (Long, Double)): (SymLong, SymDouble) = (tuple._1, tuple._2)
  implicit def longFloatTupleToSyms(tuple: (Long, Float)): (SymLong, SymFloat) = (tuple._1, tuple._2)
  
  implicit def doubleIntTupleToSyms(tuple: (Double, Int)): (SymDouble, SymInt) = (tuple._1, tuple._2)
  implicit def doubleLongTupleToSyms(tuple: (Double, Long)): (SymDouble, SymLong) = (tuple._1, tuple._2)
  implicit def doubleDoubleTupleToSyms(tuple: (Double, Double)): (SymDouble, SymDouble) = (tuple._1, tuple._2)
  implicit def doubleFloatTupleToSyms(tuple: (Double, Float)): (SymDouble, SymFloat) = (tuple._1, tuple._2)
  
  implicit def floatIntTupleToSyms(tuple: (Float, Int)): (SymFloat, SymInt) = (tuple._1, tuple._2)
  implicit def floatLongTupleToSyms(tuple: (Float, Long)): (SymFloat, SymLong) = (tuple._1, tuple._2)
  implicit def floatDoubleTupleToSyms(tuple: (Float, Double)): (SymFloat, SymDouble) = (tuple._1, tuple._2)
  implicit def floatFloatTupleToSyms(tuple: (Float, Float)): (SymFloat, SymFloat) = (tuple._1, tuple._2)
  
  
  
  // Implicits are applied in order of priority, so these should be defined last so we try to use
  // symbolics as much as possible.
  implicit def symInt2Int(s: SymInt): Int = s.getValue()
  implicit def symFloat2Float(s: SymFloat): Float = s.getValue()
  implicit def symDouble2Double(s: SymDouble): Double = s.getValue()
}

case class SymInt(value: Int, p : Provenance) extends SymBase(p) {


  // TODO: Implement the influence/rank function here
  def mergeProvenance(prov_other : Provenance): Provenance = {
   prov.cloneProvenance().merge(prov_other)
  }


  def getValue(): Int = {
    return value
  }

   def toSymString : SymString = {
    SymString(value.toString , p)
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
    new SymInt(d, prov)
  }

  def -(x: Int): SymInt = {
    val d = value - x
    new SymInt(d, prov)
  }

  def *(x: Int): SymInt = {
    val d = value * x
    new SymInt(d, prov)
  }

  def *(x: Float): SymFloat = {
    val d = value * x
    new SymFloat(d, prov)
  }


  def /(x: Int): SymDouble= {
    val d = value / x
    new SymDouble(d, prov )
  }

  def /(x: Long): SymDouble= {
    val d = value / x
    new SymDouble(d, prov)
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

  def %(x: Int): SymInt = {
    SymInt(value % x, p)
  }
  /**
    * Operators not supported yet
    */

  def ==(x: Int): Boolean = value == x

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
  def >(x: SymInt): Boolean = value > x.value

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

  def %(x: Long): Long = value % x

  def %(x: Float): Float = value % x

  def %(x: Double): Double = value % x

}
