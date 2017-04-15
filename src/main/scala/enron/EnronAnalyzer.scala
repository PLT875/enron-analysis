package enron

import parser._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD
import java.io.File;

object EnronAnalyzer {

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