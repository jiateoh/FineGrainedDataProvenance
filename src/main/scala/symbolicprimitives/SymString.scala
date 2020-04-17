package symbolicprimitives

import provenance.data.Provenance

object StringImplicits {
  implicit def symString2string(s: SymString): String = s.getValue()

}
/**
  * Created by malig on 4/29/19.
  */
case class SymString(value: String,  p: Provenance) extends SymBase(p) {


  // TODO: Implement the influence/rank function here
  def mergeProvenance(prov_other : Provenance): Provenance = {
    prov.merge(prov_other)
  }


  def getValue(): String = {
    return value
  }

  override def toString: String =
    value.toString + s""" (Provenance Bitmap: ${prov})"""

  /**
    * Unsupported Operations
    */

   def length: SymInt = {
     new SymInt( value.length, prov)
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
    new SymInt( value.toInt, prov)
  }

   def toFloat: SymFloat =
     new SymFloat(value.toFloat , prov)

   def toDouble: SymDouble ={
    new SymDouble(value.toDouble , prov)
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
