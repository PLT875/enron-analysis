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
    var msgWordCounts = Array[Double]()

    var rc = sc.parallelize(Array[(String, Double)]())
    var avWc = Array[Double]()

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
          avWc = avWc :+ averageMsgWordCount(em.get)
        }
      }
    }
    
    // need to calculate recipient scores for all Zips
    val rsAllZips = rc.reduceByKey(_ + _)

    val top100Recipients = EnronAnalyzer.topRecipients(rsAllZips, 100)
    println("--- Top 100 recipients ---")
    top100Recipients.foreach(println(_))

    // aggregated word counts of all messages per Zip
    val avgWordCount = sc.parallelize(avWc).mean.toInt
    println("--- Average word count of the emails ---")
    println(avgWordCount)

    sc.stop
  }
  
  /*
   * Calculates a total score for any valid email address, that has been included in either of the primary or 
   * carbon copy recipient lists.
   * 
   * Primary recipients have a constant score of 1.0, whereas for carbon copy recipients it is 0.5.
   * 
   * @param ems an array of email messages (that originated from an Enron Zip)
   * 
   * @return a pair RDD mapping the email address to their aggregated score
   */
  def recipientCount(ems: Array[EmailMessage])(implicit sc: SparkContext): RDD[(String, Double)] = {
    val emailRe = new Regex("""([\w-\.]+)@((?:[\w]+\.)+)([a-z]{2,4})+""")
    val toRDD = sc.parallelize(ems.flatMap(_.to))
    val ccRDD = sc.parallelize(ems.flatMap(_.cc))
    
    // maps the recipient to a given constant value
    def toConst(rec: RDD[String], const: Double): RDD[(String, Double)] = {
      rec.flatMap(_.split(",\\s*"))
        .map(emailRe.findFirstMatchIn(_))
        .filter(_ != None)
        .map(r => (r.get.toString, const))
        
    }
    
    val recipientToConst = toConst(toRDD, 1.0) ++ toConst(ccRDD, 0.5)
    
    val recipientCount = recipientToConst.reduceByKey(_ + _)
    return recipientCount
  }
  
  /*
   * Calculates the average word count for the given messages.
   * 
   * @param ems an array of email messages (that originated from an Enron Zip)
   * 
   * @return the average word count of the messages
   */
  def averageMsgWordCount(ems: Array[EmailMessage])(implicit sc: SparkContext): Double = {
    val lines = ems.flatMap(_.lines)
    val totalEmails = lines.length
    val linesRDD = sc.parallelize(lines)
    val punctRe = new Regex("\\p{Punct}")
    val blanklineRe = new Regex("""^\s*$""")

    val wordCount = linesRDD.flatMap(lines => lines)
      .map(punctRe.replaceAllIn(_, ""))
      .filter(blanklineRe.findAllIn(_).length == 0)
      .map(_.split("\\s+"))
      .map(_.length)
      .sum
      
  
    return (wordCount / totalEmails)
  }

  /*
   * Calculates the top number of email recipients ranked by their score in descending order.
   * 
   * @param rc a pair RDD mapping the email address with its aggregated scored
   * @param limit the number of results to return
   * 
   * @return an array of the top recipients
   */
  def topRecipients(rc: RDD[(String, Double)], limit: Int): Array[(String, Double)] = {
    rc.takeOrdered(limit)(Ordering[Double].reverse.on(x => x._2))
  }

}

