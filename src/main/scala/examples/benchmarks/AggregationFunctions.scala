package examples.benchmarks

import provenance.rdd.{PairProvenanceRDD, ProvenanceRDD}
import ProvenanceRDD._
import symbolicprimitives.SymImplicits._
import symbolicprimitives._

import scala.reflect.ClassTag

/** Benchmark level aggregation functions for ease of use. */
object AggregationFunctions {
  
  // based on https://stackoverflow.com/questions/3508077/how-to-define-type-disjunction-union-types
//  sealed class AggregationFunctionInput[T]
//  object AggregationFunctionInput {
//    implicit object IntWitness extends AggregationFunctionInput[Int]
//    implicit object LongWitness extends AggregationFunctionInput[Long]
//    implicit object DoubleWitness extends AggregationFunctionInput[Double]
//    implicit object FloatWitness extends AggregationFunctionInput[Float]
//    implicit object SymIntWitness extends AggregationFunctionInput[SymInt]
//    implicit object SymLongWitness extends AggregationFunctionInput[SymLong]
//    implicit object SymDoubleWitness extends AggregationFunctionInput[SymDouble]
//    implicit object SymFloatWitness extends AggregationFunctionInput[SymFloat]
//  }
  
  // Note: Many of these functions use dummy implicits to differentiate between regular and
  // symbolic functions, since type erasure makes them look identical otherwise.
  
  def sumByKey[K: ClassTag](input: ProvenanceRDD[(K, Int)]): PairProvenanceRDD[K, Int] =
    input.reduceByKey(_+_)

  def sumByKey[K: ClassTag](input: ProvenanceRDD[(K, SymInt)])
                              (implicit a: DummyImplicit): PairProvenanceRDD[K, SymInt] =
    input.reduceByKey(_+_)
  
  def averageByKey[K: ClassTag](input: ProvenanceRDD[(K, Int)]): PairProvenanceRDD[K, Double] = {
    input.aggregateByKey((0L, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
    ).mapValues({case (sum, count) => sum.toDouble/count})
  }
  def averageByKey[K: ClassTag](input: ProvenanceRDD[(K, Double)])
                               (implicit a: DummyImplicit) : PairProvenanceRDD[K, Double] = {
    input.aggregateByKey((0.0, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                                                          ).mapValues({case (sum, count) => sum.toDouble/count})
  }
  
  def averageByKey[K: ClassTag](input: ProvenanceRDD[(K, SymInt)])
                               (implicit a: DummyImplicit,
                                b: DummyImplicit): PairProvenanceRDD[K, SymDouble] = {
    input.aggregateByKey[(SymLong, SymInt)]((0L, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                                                          ).mapValues({case (sum, count) => sum.toDouble/count})
  }
  
  def averageByKey[K: ClassTag](input: ProvenanceRDD[(K, SymDouble)])
                               (implicit a: DummyImplicit,
                                b: DummyImplicit,
                                c: DummyImplicit): PairProvenanceRDD[K, SymDouble] = {
    input.aggregateByKey[(SymDouble, SymInt)]((0.0, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)}
                                                          ).mapValues({case (sum, count) => sum.toDouble/count})
  }
  
  def minMaxDeltaByKey[K: ClassTag](input: ProvenanceRDD[(K, Float)]): PairProvenanceRDD[K, Float] = {
    input.aggregateByKey((Float.MaxValue, Float.MinValue))(
      { case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next)) },
      { case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB)) })
         .mapValues({ case (min, max) => max - min })
  }
  
  def minMaxDeltaByKey[K: ClassTag](input: ProvenanceRDD[(K, SymFloat)])
                                   (implicit a: DummyImplicit): PairProvenanceRDD[K, SymFloat] = {
    input.aggregateByKey[(SymFloat, SymFloat)]((Float.MaxValue, Float.MinValue))(
      { case ((curMin, curMax), next) => (MathSym.min(curMin, next), MathSym.max(curMax, next)) },
      { case ((minA, maxA), (minB, maxB)) => (MathSym.min(minA, minB), MathSym.max(maxA, maxB)) }
      // enableUDFAwareProv = Some(false) // implied property here, since output type is tuple.
    )
    // If we didn't define udfAware flag globally, we could override enableUDFAwareProv here
    .mapValues({ case (min, max) => max - min })
  }
}
