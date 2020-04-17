import org.apache.spark.sql.SparkSession
import sparkwrapper.SparkContextWithDP
import symbolicprimitives.SymString
import symbolicprimitives.SymImplicits._

object WeatherAnalysis {

    def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder
        // .appName("Cluster WordCount (Symbolic)")
        .master("local[*]")
        .getOrCreate()
      val sc = new SparkContextWithDP(spark.sparkContext)


      val split = sc.textFileProv("weather_data").flatMap { s =>
        val tokens = s.split(",")
        // finds the state for a zipcode
        var state = zipToState(tokens(0))
        var date = tokens(1)
        // gets snow value and converts it into millimeter
        val snow = convert_to_mm(tokens(2))
        //gets year
        val year = date.substring(date.lastIndexOf('/'))
        // gets month / date
        val monthdate = date.substring(0, date.lastIndexOf('/') - 1)
        List[(SymString, Float)](
          (monthdate, snow),
          (year, snow)
        ).iterator
      }

      val deltaSnow = split.groupByKey().map { s =>
        val delta = s._2.max - s._2.min
        (s._1, delta)
      }
      deltaSnow

    }


  def convert_to_mm(s: SymString): Float = {
    val unit = s.substring(s.length - 2)
    val v = s.substring(0, s.length - 2).toFloat
    if( unit.equals("mm") ) return v else v * 304.8f
    }

  def failure(record: (SymString, Float)): Boolean = {
    record._2 > 6000f
  }

  def zipToState(str: SymString): String = {
    return (str.toInt % 50).toString
  }
}
