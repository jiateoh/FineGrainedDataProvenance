import org.apache.spark.sql.SparkSession
import sparkwrapper.SparkContextWithDP

/**
 * Cluster-based version.
 */
object ClusterWordCountDPI {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                .builder
                .appName("Cluster WordCount (DPI)")
                .getOrCreate()
    val sc = new SparkContextWithDP(spark.sparkContext)
  
  
    val input = sc.textFile(args.headOption.getOrElse("file.log"))
    val count = input.flatMap(s => s.split(' '))
                .map(s => (s,1))
                .reduceByKey(_ + _)//.filter(s => s._1.getValue().contains("ali"))
                .collect()
    
    
    // Measuring Storage overhead
    println(count.map(a => a.bitmap.getSizeInBytes).sum+ " Bytes")
    //    println(count.head)
    count.take(5).foreach(println)
  
    // Getting Provenance here
  }
}
