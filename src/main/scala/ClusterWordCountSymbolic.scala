import org.apache.spark.sql.SparkSession
import symbolicprimitives.{SymInt, SymString, Utils}

/**
  * Cluster-based version.
  */
object ClusterWordCountSymbolic {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                    .builder
                    .appName("Cluster WordCount (Symbolic)")
                    .getOrCreate()
    val sc = spark.sparkContext
    
    val input = sc.textFile(args.headOption.getOrElse("file.log"))

    val input_new = Utils.setInputZip(input.zipWithIndex()) // << Rewrites inserted
      .map(s => new SymString(s._1, s._2)) // << Rewrites Inserted

    // s x s
    val count = input_new.flatMap(s => s.split(' '))
      .map(s => (s, new SymInt(1, s.getProvenance())))
      .reduceByKey(_ + _)//.filter(s => s._1.getValue().contains("ali"))
      //.collect()
      //.take(100)
      .count()
    println(count)

    // Measuring Storage overhead
    //println(count.map(a => a._2.getProvenanceSize()).sum + " Bytes")
    //count.map(a => a._2.getProvenanceSize()).reduce(_+_) +
    //    println(count.head)
    //count.foreach(println)

    // Getting Provenance here
    //Utils.retrieveProvenance(count.head._2.getProvenance())
  }
}
