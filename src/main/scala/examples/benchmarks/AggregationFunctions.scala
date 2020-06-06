package examples.benchmarks

import provenance.rdd.{InfluenceTracker, IntStreamingOutlierInfluenceTracker, PairProvenanceRDD, ProvenanceRDD, StreamingOutlierInfluenceTracker, TopNInfluenceTracker}
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
  
  /** Default: IntStreamingOutlier */
  def sumByKeyWithInfluence[K: ClassTag](input: ProvenanceRDD[(K, Int)],
                                         influenceFn: () => InfluenceTracker[Int] =
                                             () => IntStreamingOutlierInfluenceTracker()
                                        ): PairProvenanceRDD[K, Int] =
    input.reduceByKey((a: Int, b:Int) => a + b, influenceFn)
  
  
  def averageByKey[K: ClassTag](input: ProvenanceRDD[(K, Int)],
                                enableUDFAwareProv: Option[Boolean] = None,
                                influenceTrackerCtr: Option[() => InfluenceTracker[Int]] = None): PairProvenanceRDD[K, Double] = {
    input.aggregateByKey((0L, 0))(
      {case ((sum, count), next) => (sum + next, count+1)},
      {case ((sum1, count1), (sum2, count2)) => (sum1+sum2,count1+count2)},
      enableUDFAwareProv = enableUDFAwareProv,
      influenceTrackerCtr = influenceTrackerCtr
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
  
  /** Default IntStreamingOutlier function */
  def averageByKeyWithInfluence[K: ClassTag](input: ProvenanceRDD[(K, Int)],
                                             influenceFn: () => InfluenceTracker[Int] =
                                             ()=> IntStreamingOutlierInfluenceTracker()
                                            ): PairProvenanceRDD[K, Double] = {
    averageByKey(input,
                 enableUDFAwareProv = Some(false),
                 influenceTrackerCtr = Some(influenceFn)
                 )
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
  
  /** Default StreamingOutlier */
  def averageAndVarianceByKeyWithInfluence[K: ClassTag](input: ProvenanceRDD[(K, Double)],
                                                        influenceFn: () => InfluenceTracker[Double] =
                                           () => StreamingOutlierInfluenceTracker()): PairProvenanceRDD[K, (Double, Double)] = {
    // Based on https://en.wikipedia
    // .org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    // and https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    // (based on provided python code)
    // count is initialized as a double to avoid accidental int division
    // variance returned is population variance
    val intermediate = input.aggregateByKey((0.0, 0.0, 0.0))({
      // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford
      // 's_online_algorithm
      // this is essentially the scala implementation of the python example update
      case (agg, newValue) =>
        var (count, mean, m2) = agg
        count += 1
        val delta = newValue - mean
        mean += delta / count
        val delta2 = newValue - mean
        m2 += delta * delta2
        (count, mean, m2)
    }, {
      case (aggA, aggB) =>
        // https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
        // parse values
        val (countA, meanA, m2A) = aggA
        val (countB, meanB, m2B) = aggB
        
        val count = countA + countB
        val delta = meanB - meanA
        val mean = meanA + delta * countB / count
        val m2 = m2A + m2B + (delta * delta) * (countA * countB / count)
        (count, mean, m2)
    },
     enableUDFAwareProv = Some(false),
     influenceTrackerCtr = Some(
       //                     {
       //                       val mean = 2.7437360761067904
       //                       val variance = 0.039061295613521535
       //                       val stdDev = Math.sqrt(variance)
       //                       val lower = mean - 3 * stdDev // 2.1508181555463692
       //                       val upper = mean + 3 * stdDev // 3.3366539966672115
       //                       println(s"Filter Influence tracker with range $lower to $upper")
       //                       () => FilterInfluenceTracker(value => (value <= lower) || (value
       //                       >= upper))
       //                     }
       () => StreamingOutlierInfluenceTracker()
      
       //() => FilterInfluenceTracker(value => value <= 2.3 || value >= 3.3)
       //() => TopNInfluenceTracker(5)
       //() => UnionInfluenceTracker(TopNInfluenceTracker(5), BottomNInfluenceTracker(5))
      
       //AllInfluenceTracker[Double]
       )
     )
    
    val keyedMeanVar = intermediate.mapValues({ case (count, mean, m2) =>
      // population variance is simply m2 / count
      (mean, m2 / count)
    })
    keyedMeanVar
  }
}
