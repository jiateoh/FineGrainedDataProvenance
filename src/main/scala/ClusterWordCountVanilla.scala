import org.apache.spark.sql.SparkSession

/**
  * Cluster-based version.
  */
object ClusterWordCountVanilla {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                    .builder
                    .appName("Cluster WordCount (Normal)")
                    .getOrCreate()
    val sc = spark.sparkContext
    
    val input = sc.textFile(args.headOption.getOrElse("file.log"))
    val count = input.flatMap(s => s.split(' '))
                .map(s => (s,1))
                .reduceByKey(_ + _)//.filter(s => s._1.getValue().contains("ali"))
                .collect() // intentional choice: collect information to put it on par with other
    // classes
    
    count.take(5).foreach(println)
    
  }
}
