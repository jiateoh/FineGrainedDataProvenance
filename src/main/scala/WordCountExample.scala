import org.apache.spark.{SparkConf, SparkContext}
import symbolicprimitives.{SymInt, SymString, Utils}

import scala.meta._
import main.transformers._

object WordCountExample {
  def main(args: Array[String]): Unit = {
    val program =
    """object Main extends App {
      def main(): Unit = {
        val count = input_new.flatMap(s => s.split(' '))
          .map((s) => (s, 1))
          .reduceByKey(_ + _)//.filter(s => s._1.getValue().contains("ali"))
          .collect()
      }
    }"""

    val visitor= new LiteralTransformer()
    val tree = program.parse[Source].get
    val newTree = visitor(tree)
    println(newTree)
  }
}
