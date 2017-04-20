package enron

import java.util.zip.{ ZipFile, ZipEntry }
import scala.util.matching.Regex
import util.control.Breaks._
import java.io.File._
import org.w3c.dom._
import javax.xml.parsers.DocumentBuilderFactory

/**
 * EmailMessage represents data extract from an Enron Zip file.
 *
 * @param to the primary recipients (comma delimited and unnormalized)
 * @param cc the carbon copy recipients (comma delimited and unnormalized)
 * @param lines the lines of the email message
 */
case class EmailMessage(to: Array[String], cc: Array[String])

object EnronExtractor {

  /**
   * Extracts the required email data by analyzing the XML and text files
   * in the given Zip file.
   *
   * @param zip the name of the Zip file
   *
   * @return an array of email messages
   */
  def extractEmailMessagesFromZip(zip: ZipFile): Option[EmailMessage] = {
    val entries = zip.entries
    var xmlFile: Option[String] = None
    breakable {
      // looking for an XML file to read
      while (entries.hasMoreElements) {
        val xmlRe = new Regex("(.*)?.xml")
        val file = entries.nextElement.toString
        if (xmlRe.findFirstIn(file) != None) {
          xmlFile = Some(file)
          break
        }
      }
    }

    if (xmlFile.head == None) {
      None
    }

    val inputStream = zip.getInputStream(zip.getEntry(xmlFile.head));
    val dbFactory = DocumentBuilderFactory.newInstance()
    val dBuilder = dbFactory.newDocumentBuilder()
    val document = dBuilder.parse(inputStream)
    val nodeList = document.getElementsByTagName("Tag")

    val to = getTagValues(nodeList, "#To")
    val cc = getTagValues(nodeList, "#CC")

    Some(EmailMessage(to, cc))
  }

  /**
   * Gets the TagValue of a Tag given a specified TagName.
   *
   * @param nodes - list of tag elements as a NodeList
   * @param tagName - the attribute value of the TagName
   *
   * @return an array of tag values
   */
  def getTagValues(nodes: NodeList, tagName: String): Array[String] = {
    var tagValues = Array[String]()
    for (index <- 0 until nodes.getLength) {
      val element = nodes.item(index).asInstanceOf[Element]
      val attribute = element.getAttributeNode("TagName").getValue
      if (attribute.equals(tagName)) {
        tagValues = tagValues :+ element
          .getAttributeNode("TagValue")
          .getValue
          .toLowerCase
          .trim
          .replaceAll("\\s{2,}", " ")
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

    val str = """.*\b[A-Za-z0-9]+(.[A-Za-z0-9]+)*@[A-Za-z0-9]+(.[A-Za-z0-9]+)*(.[A-Za-z]{2,})\b.*"""
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

