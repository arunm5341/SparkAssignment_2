import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date

object MainQuestion extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local").appName("assignment_2").getOrCreate()
  case class LogLine(debug_level: String, timestamp: Date, download_id: Integer,
                     retrieval_stage: String, rest: String);

  val dateFormat = "yyyy-MM-dd:HH:mm:ss"
  val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r

  val rdd = spark.sparkContext.
    textFile("src/main/resources/ghtorrent-logs.txt").
    flatMap ( x => x match {
      case regex(debug_level, dateTime, downloadId, retrievalStage, rest) =>
        val df = new SimpleDateFormat(dateFormat)
        new Some(LogLine(debug_level, df.parse(dateTime.replace("T", ":")), downloadId.toInt, retrievalStage, rest))
      case _ => None;
    })
  /*rdd.foreach(f=>{
    println(f)
  })
*/
  // 1) How many lines does the RDD contain?
  val x = rdd.count
  println(x)

  // 2) Count the number of WARNing messages
  val y = rdd.filter(_.debug_level == "WARN").count
  println(y)

  // 3) How many repositories where processed in total?
  val repos = rdd.filter(_.retrieval_stage == "api_client").
    map(_.rest.split("/").slice(4,6).mkString("/").takeWhile(_ != '?'))
  val z = repos.distinct.count
  println(z)

  // 4) Which client did most HTTP requests?
  rdd.filter(_.retrieval_stage == "api_client").
    keyBy(_.download_id).
    mapValues(l => 1).
    reduceByKey((a,b) => a + b).
    sortBy(x => x._2, false).
    take(3)


  // 5) Which client did most FAILED HTTP requests?
  rdd.filter(_.retrieval_stage == "api_client").
     filter(_.rest.startsWith("Failed")).
     keyBy(_.download_id).
     mapValues(l => 1).
     reduceByKey((a,b) => a + b).
     sortBy(x => x._2, false).
     take(3)


  // 6) What is the most active hour of day?
  rdd.keyBy(_.timestamp.getHours).
    mapValues(l => 1).
    reduceByKey((a,b) => a + b).
    sortBy(x => x._2, false).
    take(3)


  // 7) What is the most active repository?
   repos.
    filter(_.nonEmpty).
    map(x => (x, 1)).
    reduceByKey((a,b) => a + b).
    sortBy(x => x._2, false).
    take(3)

}

