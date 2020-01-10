/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import sparkwrapper.{SparkConfWithDP, SparkContextWithDP, WrappedRDD}
import trackers.Trackers

/**
 * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 *
 * This is an example implementation for learning how to use Spark. For more conventional use,
 * please refer to org.apache.spark.graphx.lib.PageRank
 *
 * Example Usage:
 * {{{
 * bin/run-example SparkPageRank data/mllib/pagerank_data.txt 10
 * }}}
 */
object SparkPageRankWithProvenance {
  
  def showWarning() {
    System.err.println(
      """WARN: This is a naive implementation of PageRank and is given as an example!
        |Please use the PageRank implementation found in org.apache.spark.graphx.lib.PageRank
        |for more conventional use.
      """.stripMargin)
  }
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }
    
    showWarning()
  
    println("Args:")
    args.foreach(println)
    
    val conf = new SparkConfWithDP()
    val spark = SparkSession
                .builder.config(conf)
                //.appName("SparkPageRankWithProvenance")
                .getOrCreate()
    
    // jteoh: change to test with UDF-Unaware API
    val sc = new SparkContextWithDP(spark.sparkContext)
    
    val iters = args.lift(1).map(_.toInt).getOrElse(10)
    // if third argument is provided, use it to set the current tracker
    args.lift(2).foreach(Trackers.setTrackerCreator)
    
    
    val file = args.lift(0).getOrElse("/Users/jteoh/Code/FineGrainedDataProvenance/part-00000")
    // val lines = spark.read.textFile(args(0)).rdd
    val lines = sc.textFile(file)
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
  
//    val temp = links.count()
//    val temp2 = links.collect().head // only safe because count is 1...
//    println("-" * 50)
//    println("Link count and provenance count and provenance size est")
//    println(temp)
//    println(temp2)
//    println(temp2.provenanceCount)
//    println(temp2.provenanceSizeEst)
//    println("-" * 50)//debugLinkCounts(links)
    
    //debugOutCounts(links)
    var ranks = links.mapValues(v => 1.0).setName("Ranks @ iteration 0")
    
    for (i <- 1 to iters) {
      val oldRanks = ranks
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
  

      // count to make sure it gets computed...
      // jt: cache requires some force computation.
//      val temp = contribs.getUnWrappedRDD().distinct()
//      println("TESTING DISTINCT ONLY: " + temp.count())
      ranks = contribs.reduceByKey({case (d1, d2) =>
        throw new Exception("BOOM")
        d1 / 0
      })
      //ranks = contribs.reduceByKey(_ + _)//.mapValues(0.15 + 0.85 * _).setName(s"Ranks @
      // iteration " + s"${i}")//.cache()
      Trackers.printDebug("Contribs count: " + contribs.count(), "Unable to compute contribs " +
        "count")
      Trackers.printDebug("Contribs Size estimate: " + contribs.getUnWrappedRDD.map(SizeEstimator
                                                                              .estimate).sum(), "Unable to" +
        " estimate RDD size")
        Trackers.printDebug("Rank counts: " + ranks.count(), t => "Unable to compute rank " +
          s"counts: $t")
          
        Trackers.printDebug("Tracker creation count: " + Trackers.count, t => "Unable to " +
            s"retrieve tracker counts: $t")
      // TODO: also test underlying reduceByKey?? or using a different bitmap impl?
//      ranks.count()
//      oldRanks.unpersist(blocking = false)
    }
  
    // Small update to extract only values from Trackers (may need to update our API instead?)
    // val output = ranks.collect()
    val output = ranks.collect().map(_.value)
    
    //output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    
    spark.stop()
  }
  
  private def debugLinkCounts(links: WrappedRDD[(String, Iterable[String])]) = {
    println("-" * 50)
    println("Debugging: countByKey for in edges")
    println("-" * 50)
    links.getUnWrappedRDD
         .map(_.value) // remove trackers
         .map(p => (p._1, p._2.size)) // get number of outgoing edges
         .reduceByKey(_ + _) // sum over outgoing neighbors
         .saveAsTextFile("/tmp/linkCounts")
  }
  
  private def debugOutCounts(links: WrappedRDD[(String, Iterable[String])]) = {
    println("-" * 50)
    println("Debugging: countByKey for outgoing edges")
    println("-" * 50)
    links.getUnWrappedRDD
         .map(_.value) // remove trackers
         .flatMap(_._2.map(out => (out, 1))) // get outgoing edges only
         .reduceByKey(_ + _) // sum over outgoing neighbors
         .saveAsTextFile("/tmp/outCounts")
  }
}
// scalastyle:on println