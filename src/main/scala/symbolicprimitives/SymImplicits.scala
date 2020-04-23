package symbolicprimitives

import provenance.data.Provenance

object SymImplicits {

  //TODO: Using zero as default provenance here. We need to chain this disconnect through dependency analysis

  implicit def int2SymInt(s: Int): SymInt = SymInt(s, Provenance.create())
  implicit def float2SymFloat(s: Float): SymFloat = SymFloat(s, Provenance.create())
  implicit def double2SymDouble(s: Double): SymDouble = SymDouble(s, Provenance.create())

  implicit def symVal2String(s: SymAny[_]): String = s.value.toString
//  implicit def symInt2String(s: SymInt): String = s.value.toString
//  implicit def symFloat2String(s: SymFloat): String = s.value.toString
//  implicit def symDouble2String(s: SymDouble): String = s.value.toString
  implicit def symString2String(s: SymString): String = s.value.toString

  implicit def symInt2SymFloat(s: SymInt): SymFloat = SymFloat(s.value, s.getProvenance())
  implicit def symFloat2SymInt(s: SymFloat): SymInt = SymInt(s.value.toInt, s.getProvenance())
  implicit def symFloat2SymDouble(s: SymFloat): SymDouble = SymDouble(s.value.toDouble, s.getProvenance())
  implicit def symInt2SymDouble(s: SymInt): SymDouble = SymDouble(s.value.toDouble, s.getProvenance())

  // A few common tuple options - these implicitly rely on the conversions defined above.
  type SymLong = SymInt //TODO we don't have a SymLong type yet, so for simplicity we use SymInt. This is *not* accurate.
  implicit def long2SymLong(s: Long): SymLong = SymInt(s.toInt, Provenance.create())
  
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
  implicit def symValToVal[T<: AnyVal](s: SymAny[T]): T = s.value
//  implicit def symInt2Int(s: SymInt): Int = s.value
//  implicit def symFloat2Float(s: SymFloat): Float = s.value
//  implicit def symDouble2Double(s: SymDouble): Double = s.value
}
