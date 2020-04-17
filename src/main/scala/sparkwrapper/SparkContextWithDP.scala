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

  def textFileProv(filepath: String): ProvenanceRDD[SymString] = {
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
}
