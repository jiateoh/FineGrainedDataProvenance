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

import org.apache.spark.{SparkConf, SparkContext}
import sparkwrapper.SparkContextWithDP
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
// $example off$

object KMeansExample {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("KMeansExample").setMaster("local[*]")
    //val sc = new SparkContext(conf)
    val sc = new SparkContextWithDP(new SparkContext(conf))
    import sc._
    
    // $example on$
    // Load and parse the data
    val data = sc.textFile("/Users/jteoh/Code/distributions/spark-2.2.0-bin-hadoop2.7/data/mllib" +
                             "/kmeans_data.txt")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()
    
    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    
    // Save and load model
    clusters.save(sc, "/tmp/KMeansModel")
    val sameModel = KMeansModel.load(sc, "/tmp/KMeansModel")
    // $example off$
    
    sc.stop()
  }
}
// scalastyle:on println
