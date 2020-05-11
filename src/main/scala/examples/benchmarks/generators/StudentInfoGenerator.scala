package examples.benchmarks.generators

package generators

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.{SparkConf, SparkContext}

/** Based off of fileGenerator.py and a scala-based parallelization script provided by Gulzar
  * (for another student project).
  * https://github.com/maligulzar/bigdebug/blob/titian-bigsift/examples/src/main/scala/org/apache/spark/examples/bigsift/benchmarks/studentdataanalysis/datageneration/fileGenerator.py
  *
  * This has been modified to include a fault injection for certain values (see the corresponding
  * method)
  */
object StudentInfoGenerator {
  def main(args:Array[String]): Unit = {
  
  
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName
    ("StudentInfoGenerator"))
    
  
    /** Copy everything below this */
    import scala.util.Random
  
    val random = new Random(42)
    /** Dataset values **/
    val ALPHABET_LIST = ('a' to 'z').toArray
    val FIRST_NAME_MAX_LENGTH = 8 // used to be 10, truncated since insignificant here
    val LAST_NAME_MAX_LENGTH = 8 // used to be 15, truncated
    val GENDER_LIST = Array("male", "female")
    val GRADE_LIST = (1 to 4).toArray
    val GRADE_TO_AGE_MAP = GRADE_LIST.map(grade => {
      // Grades are 2-year ranges based on grade level, i.e. 18-19, 19-20, 20-21, 21-22
      val lower = 18 + (grade - 1)
      val upper = lower + 1
      grade -> Array(lower, upper)
    }).toMap
    
    val faultRate = 0.0005
  
    // Fault: X% of 3rd years
    def shouldInjectFault(grade: Int): Boolean = {
      grade == 3 && random.nextDouble() <= faultRate
    }
  
    val MAJOR_LIST = Array("English", "Mathematics", "ComputerScience", "ElectricalEngineering",
                           "Business", "Economics", "Biology", "Law", "PoliticalScience",
                           "IndustrialEngineering")
  
    /** simple method for random choice. Not designed to be efficient - I hope size is precomputed! */
    
    def choice[T](coll: Seq[T]): T = {
      coll(random.nextInt(coll.size))
    }
  
    def randomAlphaString(size: Int): String = {
      (1 to size).map(_ => choice(ALPHABET_LIST)).mkString
    }
  
    /** Default Configurations **/
    /* Dataset size */
    val partitions = 10
    val partitionSize = 5 * 1000 * 1000
    // estimate: 96.1MB for 50 partitions x 50K records/part (2.5M records), so 38.44B/record
    // Targeting: 20GB -> 520,291,363 records, so we want to increase dataset by roughly 200x
    // test: 200 partitions, 2.5M records per partition
  
    /* Output file */
    val outputTarget = "datasets/studentInfo"
  
    /* Data generation */
    def generateData(mPartitions: Int, mPartitionSize: Int): Unit = {
      println(s"Creating $mPartitions partitions, $mPartitionSize records per partition")
      sc.parallelize(Seq[Int](), mPartitions).mapPartitions { _ =>
        (1 to mPartitionSize).iterator.map({ _ =>
        
          val firstNameLength = random.nextInt(FIRST_NAME_MAX_LENGTH - 3 ) + 3
          val lastNameLength = random.nextInt(LAST_NAME_MAX_LENGTH - 3) + 3
          val firstName = randomAlphaString(firstNameLength)
          val lastName = randomAlphaString(lastNameLength)
        
          val gender = choice(GENDER_LIST)
          val major = choice(MAJOR_LIST)
        
          val grade = choice(GRADE_LIST)
          var age = choice(GRADE_TO_AGE_MAP(grade))
        
          if (shouldInjectFault(grade)) {
            // Fault: grade is written twice
            // Implementation assumtion that age is 2-digits always, so we can multiply by 101
            // age * 100 + age (or age * 101)
            // More 'correctly' would involve string-concat
            // age = (age.toString + age.toString).toInt
            age = age * 101
          }
        
          s"$firstName $lastName $gender $age $grade $major"
        })
      }.saveAsTextFile(outputTarget)
    }
  
    FileUtils.deleteQuietly(new File(outputTarget))
    
    generateData(partitions, partitionSize)
  }
}