import org.apache.spark.sql.SparkSession
import provenance.data.InfluenceMarker
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.{SymInt, SymString, Utils}

/**
  * Cluster-based version.
  */
object ClusterWordCountSymbolic {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
                    .builder
                    // .appName("Cluster WordCount (Symbolic)")
        .master("local[*]")
                    .getOrCreate()
    val sc = new SparkContextWithDP(spark.sparkContext)

    val input = sc.textFileProv(("file_num.log"))
    val count =
      input.map(s => (s.split(',')(0),s.split(',')(1).toInt))
      .reduceByKey(_+_, (a,b) => if(a > b) InfluenceMarker.left else InfluenceMarker.right )
    val arr = count.collectWithProvenance()
      arr.foreach(println)

  }
}
