package enron

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD
import java.util.zip._
import scala.util.matching.Regex

import java.io.{ File, BufferedReader, InputStreamReader }

object EnronAnalyzer {

  // defining Spark to run in local mode
  val conf: SparkConf = new SparkConf().setMaster("local").setAppName("enron-analysis")
  val sc: SparkContext = new SparkContext(conf)

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

    // looking for Zip files to read in the directory
    while (dir.hasNext) {
      val zipRe = new Regex("(.*)?_xml.zip")
      val currentFile = dir.next.getAbsolutePath
      println("Current Zip being read is: " + currentFile)
      if (zipRe.findFirstIn(currentFile) != None) {
        val zipFile = new ZipFile(currentFile)
        val em = EnronExtractor.extractEmailMessagesFromZip(zipFile)
        if (em != None) {
          em.get.foreach {
            (msg) =>
              if (msg.to != None) recipientScore = recipientScore ++ extractRecipients(msg.to.get, 1.0)
              if (msg.cc != None) recipientScore = recipientScore ++ extractRecipients(msg.cc.get, 0.5)
              msgWordCounts = msgWordCounts :+ msg.msgWordCount
          }
        }
      }
    }

    val recipientsRDD = sc.parallelize(recipientScore)

    val topRecipients = EnronAnalyzer.topRecipients(recipientsRDD, 100)
    topRecipients.foreach(println(_))

    val wordCountsRDD = sc.parallelize(msgWordCounts)

    val avg = avgWordCounts(wordCountsRDD)
    println(avg)

    sc.stop
    // while(!sc.isStopped) {
    // keeping the application alive so that Spark UI is still available
    // }

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
    r.reduceByKey(_ + _).takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
  }

  def avgWordCounts(wc: RDD[Int]): Double = {
    wc.mean().toInt
  }
}

