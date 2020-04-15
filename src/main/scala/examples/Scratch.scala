package examples

import org.apache.spark.util.SizeEstimator
import provenance.data.{DummyProvenance, Provenance, RoaringBitmapProvenance}

import scala.reflect.{ClassTag, classTag}
import reflect.runtime.universe._

class FakeRDD[T: TypeTag](data: Seq[T]) {
  val tType = typeOf[T]
  def map[U: ClassTag](fn: T => U): Seq[U] = {
//    val uType = typeOf[U]
    val targs = tType match {
      case TypeRef(_, _, args) => args
    }
    println(s"type of $fn has type arguments $targs")
    data.map(fn)
  }
}

object Scratch extends App {
  val data = 1 to 5
  def testMap[U](fn: Int => U)(implicit ct: ClassTag[U]) = {
    val StringTag = classTag[String]
    val IntTag = classTag[Int]
    ct match {
      case StringTag =>
        println("I found a string?")
      case IntTag =>
        println("Int!")
      case _ =>
        println("Darn")
    }
    data.map(fn)
  }
  
//  def testMap(fn: Int => Int) = {
//    println("Awesome!")
//    data.map(fn)
//  }
  
  testMap(x => x + 1).foreach(println)
  testMap(x => x + "1").foreach(println)
  testMap(x => Seq()).foreach(println)
  
  val mockData: Seq[(Int, Provenance)] = Seq((1, DummyProvenance.create()), (2,
    DummyProvenance.create()))
  
  new FakeRDD(mockData).map(x => x._1 + 1).foreach(println)
  
  println("Size estimates")
  val prov = RoaringBitmapProvenance.create(1)
  println(SizeEstimator.estimate(prov))
  println(prov.estimateSize)
  //println(SizeEstimator.estimate(prov.hashCode()))
}
