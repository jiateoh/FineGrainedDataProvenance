package examples.benchmarks

import provenance.rdd.{PairProvenanceRDD, ProvenanceRDD}
import symbolicprimitives.SymImplicits._
import symbolicprimitives._

object AggregationFunctions {
  // based on https://stackoverflow.com/questions/3508077/how-to-define-type-disjunction-union-types
  sealed class AggregationFunctionInput[T]
  object AggregationFunctionInput {
    implicit object IntWitness extends AggregationFunctionInput[Int]
    implicit object LongWitness extends AggregationFunctionInput[Long]
    implicit object DoubleWitness extends AggregationFunctionInput[Double]
    implicit object FloatWitness extends AggregationFunctionInput[Float]
    implicit object SymIntWitness extends AggregationFunctionInput[SymInt]
    implicit object SymLongWitness extends AggregationFunctionInput[SymLong]
    implicit object SymDoubleWitness extends AggregationFunctionInput[SymDouble]
    implicit object SymFloatWitness extends AggregationFunctionInput[SymFloat]
  }
  
  def minMaxDelta(split: ProvenanceRDD[(String, Float)]): PairProvenanceRDD[String, Float] = {
    split.aggregateByKey((0F, 0F))(
      { case ((curMin, curMax), next) => (Math.min(curMin, next), Math.max(curMax, next)) },
      { case ((minA, maxA), (minB, maxB)) => (Math.min(minA, minB), Math.max(maxA, maxB)) })
         .mapValues({ case (min, max) => max - min })
  }
  
  def minMaxDeltaSym(split: ProvenanceRDD[(SymString, SymFloat)]): PairProvenanceRDD[SymString, SymFloat] = {
    split.aggregateByKey[(SymFloat, SymFloat)]((0F, 0F))(
      { case ((curMin, curMax), next) => (MathSym.min(curMin, next), MathSym.max(curMax, next)) },
      { case ((minA, maxA), (minB, maxB)) => (MathSym.min(minA, minB), MathSym.max(maxA, maxB)) })
         // If we didn't define udfAware flag globally, we could override enableUDFAwareProv here
         .mapValues({ case (min, max) => max - min })
  }
}
