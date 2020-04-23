package sparkwrapper

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import provenance.data.Provenance
import provenance.rdd.{FlatProvenanceDefaultRDD, ProvenanceRDD}
import symbolicprimitives.{SymString, Utils}

import scala.reflect.ClassTag

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(sc: SparkContext) {


  def textFile(filepath: String): RDD[String] ={
    sc.textFile(filepath)
  }
  
  private def textFileProvenanceCreator[T: ClassTag](filepath: String,
                                           followup: (String, Provenance) => T): RDD[T] = {
    val rdd = sc.textFile(filepath)
    // Ugly note: we need to have a handle to the actual provenance factory in case we've
    // adjusted this in our application (since the change needs to be propagated to all
    // machines in the cluster). Provenance.create() by itself will lazily evaluate
    // provenanceFactory, which won't be updated on worker nodes if changed in-application.
    val provCreatorFn = Provenance.createFn()
    Utils.setInputZip(rdd.zipWithUniqueId())
         .map { record =>
                  val prov = provCreatorFn(Seq(record._2))
                  followup(record._1, prov)
              }
  }
  
  /** Text file with Symbolic strings (no provenance RDD) */
  def textFileUDFProv(filepath: String): RDD[SymString] = {
    textFileProvenanceCreator(filepath, SymString)
  }

  /** Text file with symbolic strings and provenance RDDs. */
  def textFileSymbolic(filepath: String): ProvenanceRDD[SymString] = {
    if(!Utils.getUDFAwareEnabledValue(None)) {
      // TODO jteoh: we might be able to remove this warning if we determine at collect-time that
      //  the output we are collecting is a symbolic type?
      println("WARNING: Did you mean to enable UDF Aware provenance since you are using " +
                "textFileSymbolic?")
    }
    val baseRDD = textFileProvenanceCreator(filepath, (str, prov) => (SymString(str, prov), prov))
    new FlatProvenanceDefaultRDD(baseRDD)
  }
  
  /** Text file with provenance RDDs (but no symbolic strings) */
  def textFileProv(filepath: String): ProvenanceRDD[String] = {
    // have to define this because predef identity was detecting as Nothing => Nothing
    val identity = (s: String, p: Provenance) => (s, p)
    val baseRDD = textFileProvenanceCreator(filepath, identity)
    new FlatProvenanceDefaultRDD[String](baseRDD)
  }
}
