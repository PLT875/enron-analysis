package enron

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import java.util.zip._
import scala.util.matching.Regex

import java.io.{ File, BufferedReader, InputStreamReader }

object EnronAnalyzer {

  // defining Spark to run in local mode
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("enron-analysis")
  implicit val sc: SparkContext = new SparkContext(conf)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {
      println("Usage: sbt run <directory>")
      println("-- <directory> -- the absolute path of the enron data directory")
      System.exit(0)
    }

    sys.ShutdownHookThread {
      println("Exiting")
      Thread.sleep(1000)
      println("Shutting down Spark")
      sc.stop
    }

    val dir = new File(args(0)).listFiles().toIterator

    var recipientScore = Array[(String, Double)]()
    var msgWordCounts = Array[Int]()

    var rc = sc.parallelize(Array[(String, Double)]())
    var wc = sc.parallelize(Array[Int]())

    // looking for Zip files to read in the directory
    while (dir.hasNext) {
      val zipRe = new Regex("(.*)?_xml.zip")
      val currentFile = dir.next.getAbsolutePath
      if (zipRe.findFirstIn(currentFile) != None) {
        println("Current Zip being read is: " + currentFile)
        val zipFile = new ZipFile(currentFile)
        val em = EnronExtractor.extractEmailMessagesFromZip(zipFile)
        if (em != None) {
          rc = rc ++ recipientCount(em.get)
          wc = wc ++ messageCount(em.get)
        }
      }
    }

    val top100Recipients = EnronAnalyzer.topRecipients(rc, 100)
    println("--- Top 100 recipients ---")
    top100Recipients.foreach(println(_))

    val avgWordCount = EnronAnalyzer.avgWordCounts(wc)
    
    println("--- Average word count of the emails ---")
    println(avgWordCount)

    sc.stop
  }

  def recipientCount(ems: Array[EmailMessage])(implicit sc: SparkContext): RDD[(String, Double)] = {
    val emailRe = new Regex("""([\w-\.]+)@((?:[\w]+\.)+)([a-z]{2,4})+""")
    val toRDD = sc.parallelize(ems.flatMap(_.to))
    val ccRDD = sc.parallelize(ems.flatMap(_.cc))
    def parseAndCount(rec: RDD[String], const: Double): RDD[(String, Double)] = {
      rec.flatMap(_.split(",\\s*"))
        .map(emailRe.findFirstMatchIn(_))
        .filter(_ != None)
        .map(r => (r.get.toString, const))
        .reduceByKey(_ + _)
    }

    return (parseAndCount(toRDD, 1.0) ++ parseAndCount(ccRDD, 0.5))
  }

  def messageCount(ems: Array[EmailMessage])(implicit sc: SparkContext): RDD[Int] = {
    val lines = ems.flatMap(_.lines)
    val linesRDD = sc.parallelize(lines)

    def parseAndCount(l: RDD[Array[Object]]): RDD[Int] = {
      val wordCount = l.flatMap(lines => lines)
        .map(line => line.toString)
        .map(_.split("\\s+"))
        .map(_.length)

      return wordCount
    }
    
    return parseAndCount(linesRDD)
  }

  /**
   * Extract recipients from a given comma separated string.
   *
   * @param recipients list of recipients that are comma separated
   * @param score a constant value to associate each recipient with
   *
   * @return an array of tuples mapping a recipient to the specified score
   */
  def extractRecipients(recipients: String, score: Double): Array[(String, Double)] = {
    recipients.split(",\\s*").map(r => (r, score))
  }

  def topRecipients(r: RDD[(String, Double)], limit: Int): Array[(String, Double)] = {
    r.takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
  }

  def avgWordCounts(wc: RDD[Int]): Double = {
    wc.mean().toInt
  }

  def removePunctuation(target: String): String = {
    val regex = new Regex("\\p{Punct}")
    regex.replaceAllIn(target, "")
  }
}

