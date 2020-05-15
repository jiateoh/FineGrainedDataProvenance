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
     SymInt(value.length, getProvenance())
  }

   def split(separator: Char): Array[SymString] = {
    value
      .split(separator)
      .map(s =>
         SymString(
          s, getProvenance()))
  }
  def split(regex: String): Array[SymString] = {
    value
      .split(regex)
      .map(s =>
         SymString(
          s, getProvenance()))
  }
   def split(separator: Array[Char]): Array[SymString] = {

    value
      .split(separator)
      .map(s =>
         SymString(
          s, getProvenance()
        ))
  }

  def substring(arg0: SymInt): SymString = {
      SymString(value.substring(arg0.value), newProvenance(arg0.getProvenance()))
  }

  def substring(arg0: Int, arg1: SymInt): SymString = {
    SymString(value.substring(arg0, arg1.value), newProvenance(arg1.getProvenance()))
  }
  def substring(arg0: SymInt, arg1: SymInt): SymString = {
    SymString(value.substring(arg0.value, arg1.value), newProvenance(arg0.getProvenance(), arg1.getProvenance()))
  }

  def lastIndexOf(elem: Char): SymInt = {
    SymInt(value.lastIndexOf(elem), getProvenance())
  }


   def toInt: SymInt ={
    SymInt( value.toInt, getProvenance())
  }

   def toFloat: SymFloat =
     SymFloat(value.toFloat , getProvenance())

   def toDouble: SymDouble ={
    SymDouble(value.toDouble , getProvenance())
  }

  def equals(obj: SymString): Boolean = value.equals(obj.value)
  def eq(obj: SymString): Boolean = value.eq(obj.value)

}
