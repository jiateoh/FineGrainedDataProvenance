package examples

import org.apache.spark.sql.SparkSession
import provenance.data.Provenance
import sparkwrapper.{SparkConfWithDP, SparkContextWithDP}

object ScratchRDD extends App {
  val iters = 20
  
  Provenance.setProvenanceType("bitmap")
  
  val conf = new SparkConfWithDP().setMaster("local[*]")
  val spark = SparkSession
    .builder.config(conf)
    .appName("ScratchRDD")
    .getOrCreate()
  
  spark.sparkContext.setLogLevel("WARN")
  val sc = new SparkContextWithDP(spark.sparkContext)
  val file = args.lift(0).getOrElse("/Users/jteoh/Code/FineGrainedDataProvenance/testing_graph_pr" +
                                      ".txt")
  val lines = sc.textFileProv(file) // TODO: Formalize this into textFile
  val links = lines.map{ s =>
    val parts = s.split("\\s+")
    (parts(0), parts(1))
  }.distinct().groupByKey().cache()
  
  var ranks = links.mapValues(v => 1.0).setName("Ranks @ iteration 0")
  
  for (i <- 1 to iters) {
    println(s"==========Iteration $i=========")
    val oldRanks = ranks
    val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
      val size = urls.size
      urls.map(url => (url, rank / size))
    }
    
    
    ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
                    .setName(s"Ranks @ iteration $i").cache()
    ranks.count()
    oldRanks.unpersist(blocking = false)
  }
  
  ranks.collect()
}
