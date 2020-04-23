package symbolicprimitives

import provenance.data.Provenance

/**
  * Created by malig on 4/29/19.
  */
case class SymString(override val value: String, p: Provenance) extends SymAny(value, p) {

  /**
    * Unsupported Operations
    */

   def length: SymInt = {
     SymInt(value.length, prov)
  }

   def split(separator: Char): Array[SymString] = {
    value
      .split(separator)
      .map(s =>
         SymString(
          s, prov))
  }
  def split(regex: String): Array[SymString] = {
    value
      .split(regex)
      .map(s =>
         SymString(
          s, prov))
  }
   def split(separator: Array[Char]): Array[SymString] = {

    value
      .split(separator)
      .map(s =>
         SymString(
          s, prov
        ))
  }

  def substring(arg0: SymInt): SymString = {
      SymString(value.substring(arg0.value), mergeProvenance(arg0.prov))
  }

  def substring(arg0: Int, arg1: SymInt): SymString = {
    SymString(value.substring(arg0, arg1.value), mergeProvenance(arg1.prov))
  }
  def substring(arg0: SymInt, arg1: SymInt): SymString = {
    SymString(value.substring(arg0.value, arg1.value), mergeProvenance(arg1.prov))
  }

  def lastIndexOf(elem: Char): SymInt = {
    SymInt(value.lastIndexOf(elem),prov)
  }


   def toInt: SymInt ={
    SymInt( value.toInt, prov)
  }

   def toFloat: SymFloat =
     SymFloat(value.toFloat , prov)

   def toDouble: SymDouble ={
    SymDouble(value.toDouble , prov)
  }

  def equals(obj: SymString): Boolean = value.equals(obj.value)
  def eq(obj: SymString): Boolean = value.eq(obj.value)

}
