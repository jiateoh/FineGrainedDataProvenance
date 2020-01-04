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
import sparkwrapper.{SparkConfWithDP, SparkContextWithDP}

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
    
    val iters = if (args.length > 1) args(1).toInt else 10
    
    val file = args.lift(0).getOrElse("/Users/jteoh/Code/FineGrainedDataProvenance/part-00000")
    // val lines = spark.read.textFile(args(0)).rdd
    val lines = sc.textFile(args(0))
    val links = lines.map{ s =>
      val parts = s.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
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
      ranks = contribs.reduceByKey(_ + _)//.mapValues(0.15 + 0.85 * _).setName(s"Ranks @
      // iteration " + s"${i}")//.cache()
      println("TESTING new-ranks ONLY: " + ranks.count())
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
}
// scalastyle:on println
