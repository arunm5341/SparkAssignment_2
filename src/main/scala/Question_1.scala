import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Question_1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").appName("assignment_2").getOrCreate()

    val rdd = spark.sparkContext.textFile("src/main/resources/ghtorrent-logs.txt")
    rdd.foreach(f=>{
      println(f)
    })


  }
}
