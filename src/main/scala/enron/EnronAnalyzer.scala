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

    val dir = new File(args(0)).listFiles().toIterator
    while (true) {
      // looking for Zip files to read in the directory
      while (dir.hasNext) {
        val zipRe = new Regex("(.*)?.zip")
        val currentFile = dir.next.getAbsolutePath
        if (zipRe.findFirstIn(currentFile) != None) {
          val zipFile = new ZipFile(currentFile)
          println(currentFile)
          val em = EnronExtractor.extractEmailMessagesFromZip(zipFile)
          em.head.to.foreach(println(_))
          //val zipEntries = zipFile.entries
          //while (zipEntries.hasMoreElements) {
          //println(zipEntries.nextElement.getName)
          //}
          //val i = zipFile.getInputStream(zipFile.getEntry("test/a.txt"))

          //val a = new BufferedReader(new InputStreamReader(i));
          //val b = a.lines()
          //b.toArray().foreach(println(_))

        }

      }
    }
    //sc.stop()
  }
}

/*
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("enron-analysis")
    val sc: SparkContext = new SparkContext(conf)
    
    val directory = new File("/Users/peter.tran/Downloads/Development/enron")
    val fileList = directory.listFiles().toIterator
    
    
    var emailsRDD = sc.parallelize(Array[(String, Double)]())
    while (fileList.hasNext) {
      var emails = XmlParser.extractEmailDataFromXml(fileList.next.getAbsolutePath)
      var e = sc.parallelize(emails.get)
      emailsRDD = emailsRDD ++ e
    }
    
    topRecipients(emailsRDD, 100).foreach(println(_))
    
    sc.stop()
  }
  
  def topRecipients(r: RDD[(String, Double)], limit: Int) : Array[(String, Double)] = {
    r.reduceByKey(_ + _).takeOrdered(100)(Ordering[Double].reverse.on(x => x._2))
  }

}

class EnronAnalyzer {
    
}
*/