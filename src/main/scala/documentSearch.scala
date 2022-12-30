import com.sun.tools.javac.util.StringUtils
import org.apache.spark.sql.functions._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrameReader, Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.commons.lang.StringUtils
import org.apache.log4j.Logger
import org.apache.log4j.Level
import collection.JavaConversions._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit
import java.util.logging.{Level, LogManager, Logger}
import java.util.{Calendar, HashMap, List, Map}
import java.util.regex.{Matcher, Pattern}

object documentSearch {
  lazy val log = LogManager.getLogger("documentSearch")

  def main(args: Array[String]): Unit = {

    val appName = "documentSearch"
    var exitCode: Int = 0

    val inputPath = args(0)

    val  sparkSession  = SparkSession
      .builder()
      .appName("Getfirefly")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.rpc.netty.dispatcher.numThreads", "2")
      .config("spark.shuffle.compress", "true")
      .config("spark.rdd.compress", "true")
      .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
      .config("spark.io.compression.codec", "lzf")
      .config("spark.broadcast.compress", "true")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.parallel", "true")
      .config("hive.auto.convert.join", "true")
      .config("hive.vectorized.execution.reduce.enabled", "true")
      .config("hive.vectorized.execution.enabled", "true")
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    try {
      log.info("Program Started Running")
      //**
      excecuteMain(sparkSession, inputPath)
      //**
    }
    catch {
      case t: Throwable => log.error("Exception thrown", t); exitCode = 1
    }
    finally {
      sparkSession.stop()
    }
    System.exit(exitCode)
  }


  def excecuteMain(sparkSession: SparkSession, inputPath: String): Unit = {

    import sparkSession.sqlContext.implicits._

    //* Create file path and content array
    val fileContent = sparkSession.sparkContext.wholeTextFiles(inputPath).collect()

    println("Please enter the search term:")
    var search =  scala.io.StdIn.readLine()
    println("Select any Search Method: 1) String Match 2) Regular Expression 3) IndexedSearch ")
    var method =  scala.io.StdIn.readLine()

    //   val search: String = "document"
    println ("Searching the entered term: " + search)
    val termDf = search.split(Array(' ')).toList.toDF()
    var multiword: Boolean = false;

    if (termDf.count() != 1){
      multiword  = true;}

    var builder = StringBuilder.newBuilder
    builder.append("Search results:-" + "\n")

    var elapsedtime: Long  = 0;

    //    val fileText = sparkSession.sparkContext.textFile("file:///home_dir/svgem001/performance/millionrandomword.txt").collect()
    //    val method = "1"

    for (fname <- fileContent){
      val fileName  = fname._1.toString
      val fileText  = sparkSession.sparkContext.parallelize(Seq(fname._2))
      var countFind = 0

      if (method == "1")
      {
        val startTime: LocalDateTime = LocalDateTime.now
        //   val lineList  = fileText.flatMap(lines=>lines.split("(?<=[a-z])\\.\\s+")).toList
        //* Spilt document by Line and create the list
        val lineList  = fileText.flatMap(lines=>lines.split("\n")).collect().toList

        countFind = excecuteSearch(lineList, fileName)

        val diff = ChronoUnit.MILLIS.between(startTime, LocalDateTime.now)
        elapsedtime = elapsedtime + diff
      }
      else if (method == "2")
      {
        val startTime: LocalDateTime = LocalDateTime.now
        val regexpr = Pattern.compile("^.*" + search + ".*$")
        val lineDet = fileText.flatMap(x=>x.split("\n"))
        val onlyLine = lineDet.map(x => x).filter(x => regexpr.matcher(x).matches()).map(word=>(word)).collect().toList

        countFind = excecuteSearch(onlyLine, fileName)

        val diff = ChronoUnit.MILLIS.between(startTime, LocalDateTime.now)
        elapsedtime = elapsedtime + diff
      }
      else if (method == "3")
      {
        //*  Split the document and index on every line. Array[(String, Long)
        val words = fileText.flatMap(line => line.split("\n")).zipWithIndex().distinct()

        //*  convert line into dataset. [line: string, key: bigint]
        val lineKeyDf  = words.toDF("line", "key").sort("key").filter($"line" =!= "")

        //* Create word and linenumber index dataframe. [word: string, index: array<bigint>]
        val wordkeyDf = words.flatMap { case (value, key) => value.trim.split( " ")
          .map(word => (word, key))
        }.groupByKey.mapValues(_.toSet.toSeq).toDF("word", "index")

        //* Search the term and find the line numbers from word index.  [_1: bigint, _2: string]
        val matchedWRow =
          if  (multiword == true) {
            wordkeyDf.join(termDf, termDf("value") === wordkeyDf("word")).drop("key").withColumn("index", explode($"index")).rdd.map{
              case row => (row.getAs[Long] ("index"), row.getAs[String] ("word"))
            }.groupByKey.mapValues(i => i.toList.mkString(",")).toDF
          } else {
            wordkeyDf.withColumn("boolean",col("word").rlike(search.toString())).filter("boolean = true").drop("boolean").withColumn("index", explode($"index")).rdd.map{
              case row => (row.getAs[Long] ("index"), row.getAs[String] ("word"))
            }.groupByKey.mapValues(i => i.toList.mkString(",")).toDF
          }


        //*  Filter the lines by matching line key where search term matched
        if (matchedWRow.take(1).length > 0) {
          val startTime: LocalDateTime = LocalDateTime.now
          val matchLRow = lineKeyDf.join(matchedWRow, lineKeyDf("key") ===  matchedWRow("_1")).select("line").collect().map(r => r(0).toString).toList
          countFind = excecuteSearch(matchLRow, fileName)
          val diff = ChronoUnit.MILLIS.between(startTime, LocalDateTime.now)
          elapsedtime = elapsedtime + diff
        } else {
          val countFind = 0
        }

      }
      if (countFind >= 1)
      {builder.append(fileName + ": " + countFind + " matches"+"\n")}
      else
      {builder.append(fileName + ": " + "No matches found"+ "\n")}
    }
    builder.append("Elapsed time: "  + elapsedtime + "ms"+ "\n")
    println(builder.toString())

    def excecuteSearch (lineList: List[String], fileName: String ): Int = {
      import collection.JavaConversions._
      var count = 0;
      for ( x: String <- lineList) {
        if (x.contains(search))
        {
          val countMatch = StringUtils.countMatches(x.intern(), search)
          count = count + countMatch
        }
      }
      return count;
    }
  }

  log.info("Job finished at:" + System.currentTimeMillis())
}
