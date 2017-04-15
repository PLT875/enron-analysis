package parser

import java.util.zip.ZipFile
import scala.util.matching.Regex
import util.control.Breaks._
import java.io.File._
import org.w3c.dom._
import javax.xml.parsers.{ DocumentBuilderFactory, DocumentBuilder }

object XmlParser {

  /**
   * Reads an XML file from a single zip to extract required email data.
   *
   * @param zip - the zip file name
   *
   * @return an array of recipients and scores (1.0 for #To and 0.5 for #CC)
   */

  def extractEmailDataFromXml(zip: String): Option[Array[(String, Double)]] = {
    val re = new Regex("(.*)?.xml")
    val zipFile = new ZipFile(zip)
    val entries = zipFile.entries
    var xmlFile: Option[String] = None
    breakable {
      while (entries.hasMoreElements) {
        val file = entries.nextElement.toString
        if (re.findFirstIn(file) != None) {
          xmlFile = Some(file)
          break
        }
      }
    }

    if (xmlFile.head == None) {
      None
    }

    val inputStream = zipFile.getInputStream(zipFile.getEntry(xmlFile.head));
    val dbFactory = DocumentBuilderFactory.newInstance()
    val dBuilder = dbFactory.newDocumentBuilder()
    val document = dBuilder.parse(inputStream)
    val nodeList = document.getElementsByTagName("Tag")
    
    val to = tagValue(nodeList, "#To").map((_, 1.0))
    val cc = tagValue(nodeList, "#CC").map((_, 0.5))

    Some(to ++ cc)
  }

  /**
   * Extracts the TagValue of a Tag given a specified TagName.
   *
   * @param nodes - represents the tags elements as a NodeList
   * @param tagName - the attribute value of the TagName
   *
   * @return an array of tag values
   */
  def tagValue(nodes: NodeList, tagName: String): Array[String] = {
    var tagValues = Array[String]()
    for (index <- 0 until nodes.getLength) {
      val element = nodes.item(index).asInstanceOf[Element]
      val attribute = element.getAttributeNode("TagName").getValue
      if (attribute.equals(tagName)) {
        tagValues = tagValues ++ element
          .getAttributeNode("TagValue")
          .getValue
          .toLowerCase
          .trim
          .replaceAll("\\s{2,}", " ").split(",\\s*")
      }
    }

    tagValues
  }

  /**
   * N.B. Doesn't work properly.
   *
   * Normalizes the given recipient to a valid email address
   *
   * @param recipient - an unnormalized recipient, e.g. valderrama larry <larry.valderrama@enron.com>
   *
   * @return a valid email address, e.g. larry.valderrama@enron.com
   */
  def normalizeEmailAddress(recipient: String): String = {
    val matchFirstLastName = """(.+)?\s+?(.+)""".r
    val matchLdap = """.*recipients\/cn=(\w+)[>|\/].*""".r

    val str = """\b[A-Za-z0-9]+(.[A-Za-z0-9]+)*@[A-Za-z0-9]+(.[A-Za-z0-9]+)*(.[A-Za-z]{2,})\b"""
    val findEmailRe = new Regex(str)

    val ldapMatch = matchLdap.findFirstMatchIn(recipient)
    if (ldapMatch != None) {
      return ldapMatch.get.group(1) ++ "@enron.com"
    }

    val findEmailAddr = findEmailRe.findFirstIn(recipient)
    if (findEmailAddr != None) {
      return findEmailAddr.get
    }

    val nameMatch = matchFirstLastName.findFirstMatchIn(recipient)
    if (nameMatch != None) {
      return nameMatch.get.group(1) ++ "." ++ nameMatch.get.group(2) ++ "@enron.com"
    }

    recipient
  }
}

