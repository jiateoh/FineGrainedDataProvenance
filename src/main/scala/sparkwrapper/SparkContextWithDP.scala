package sparkwrapper

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import provenance.data.Provenance
import provenance.rdd.{FlatProvenanceDefaultRDD, ProvenanceRDD}
import symbolicprimitives.{SymString, Utils}

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(sc: SparkContext) {


  def textFile(filepath: String): RDD[String] ={
    sc.textFile(filepath)
  }
  
  /** Text file with Symbolic strings (no provenance RDD) */
  def textFileUDFProv(filepath: String): RDD[SymString] = {
    val rdd = sc.textFile(filepath)
    // This needs to be called outside to ensure cluster usage works with the right factory
    // Previously I used Provenance.create for a counter, but it's unreliable for cluster usage
    val provCreatorFn = Provenance.provenanceFactory.create _
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .map { record =>
        val prov = provCreatorFn(record._2)
        (SymString(record._1, prov))
      }
    tracked_rdd
  }

  /** Text file with symbolic strings and provenance RDDs. */
  def textFileSymbolic(filepath: String): ProvenanceRDD[SymString] = {
    if(!Utils.getUDFAwareEnabledValue(None)) {
      // TODO jteoh: we might be able to remove this warning if we determine at collect-time that
      //  the output we are collecting is a symbolic type?
      println("WARNING: Did you mean to enable UDF Aware provenance since you are using " +
                "textFileSymbolic?")
    }
    val rdd = sc.textFile(filepath)
    // This needs to be called outside to ensure cluster usage works with the right factory
    // Previously I used Provenance.create for a counter, but it's unreliable for cluster usage
    val provCreatorFn = Provenance.provenanceFactory.create _
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .map{
        input =>
          val prov = provCreatorFn(input._2)
          (SymString(input._1, prov), prov)}
    new FlatProvenanceDefaultRDD[SymString](tracked_rdd)
  }
  
  /** Text file with provenance RDDs (but no symbolic strings) */
  def textFileProv(filepath: String): ProvenanceRDD[String] = {
    val rdd = sc.textFile(filepath)
    // This needs to be called outside to ensure cluster usage works with the right factory
    // Previously I used Provenance.create for a counter, but it's unreliable for cluster usage
    val provCreatorFn = Provenance.provenanceFactory.create _
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .map{
        input =>
          val prov = provCreatorFn(input._2)
          (input._1, prov)
      }
    new FlatProvenanceDefaultRDD[String](tracked_rdd)
  }
}
