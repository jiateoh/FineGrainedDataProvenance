package sparkwrapper

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.roaringbitmap.RoaringBitmap
import provenance.data.Provenance
import provenance.rdd.{FlatProvenanceDefaultRDD, ProvenanceRDD}
import symbolicprimitives.Utils
import trackers.{BaseTracker, RoaringBitmapTracker, Trackers}

/**
  * Created by malig on 12/3/19.
  */
class SparkContextWithDP(sc: SparkContext) {
  /** TODO setting provenance type doesn't seem to work in a configurable manner yet */
//  private val conf: SparkConf = sc.getConf
//  conf.set("Hello world", "foobar")
//  Provenance.printDebug(conf.getAll.mkString("\n"), "??")
//  val provType = conf.get("provenance.type", "bitmap")
//  Provenance.printDebug(s"Setting provenance type to $provType", "")
//  Provenance.setProvenance(provType)
  
  def textFile(filepath: String): WrappedRDD[String] = {
    val rdd = sc.textFile(filepath)
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .map({case (value, id) => Trackers.createTracker(value, id)})
    new WrappedRDD[String](tracked_rdd)
  }
  
  def textFileProv(filepath: String): ProvenanceRDD[String] = {
    val rdd = sc.textFile(filepath)
    // This needs to be called outside to ensure cluster usage works with the right factory
    // Previously I used Provenance.create for a counter, but it's unreliable for cluster usage
    val provCreatorFn = Provenance.provenanceFactory.create _
    val tracked_rdd = Utils
      .setInputZip(rdd.zipWithUniqueId())
      .mapValues(provCreatorFn)
    new FlatProvenanceDefaultRDD[String](tracked_rdd)
  }
  
  
  
}