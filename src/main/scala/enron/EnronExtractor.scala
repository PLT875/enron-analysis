package enron

import java.util.zip.{ ZipFile, ZipEntry }
import scala.util.matching.Regex
import util.control.Breaks._
import java.io.{ File, BufferedReader, InputStreamReader }
import org.w3c.dom._
import javax.xml.parsers.DocumentBuilderFactory
import java.util.Arrays

/**
 * EmailMessage represents data extract from an Enron Zip file.
 *
 * @param to the primary recipients (comma delimited and unnormalized)
 * @param cc the carbon copy recipients (comma delimited and unnormalized)
 * @param msgWordCount the word count of the email message
 */
case class EmailMessage(to: Option[String], cc: Option[String], msgWordCount: Int)

object EnronExtractor {

  /**
   * Extracts the required email data by analyzing the XML and text files
   * in the given Zip file.
   *
   * @param zip the name of the Zip file
   *
   * @return an array of email messages
   */
  def extractEmailMessagesFromZip(zip: ZipFile): Option[Array[EmailMessage]] = {
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

    if (xmlFile == None) {
      return None
    }

    val inputStream = zip.getInputStream(zip.getEntry(xmlFile.head));
    val dbFactory = DocumentBuilderFactory.newInstance()
    val dBuilder = dbFactory.newDocumentBuilder()
    val document = dBuilder.parse(inputStream)

    val docList = document.getElementsByTagName("Document")

    Some(extractEmailMessagesFromXml(docList, zip))
  }

  /**
   * Extracts the required email data from the XML file.
   *
   * @param documentList the document elements as a NodeList
   *
   * @return an array of email messages
   */
  def extractEmailMessagesFromXml(documentList: NodeList, zip: ZipFile): Array[EmailMessage] = {
    var emailMessages = Array[EmailMessage]()
    for (index <- 0 until documentList.getLength) {
      val currentDoc = documentList.item(index).asInstanceOf[Element]
      val docType = currentDoc.getAttributeNode("DocType").getValue
      if (docType == "Message") {
        val tagList = currentDoc.getElementsByTagName("Tag")
        val to = getTagValue(tagList, "#To")
        val cc = getTagValue(tagList, "#CC")

        val fileList = currentDoc.getElementsByTagName("File")
        val msgWordCount = getMessageWordCount(fileList, zip)

        emailMessages = emailMessages :+ EmailMessage(to, cc, msgWordCount)
      }
    }

    emailMessages
  }

  /**
   * Gets the TagValue of a Tag given a specified TagName.
   *
   * @param tagList the tag elements as a NodeList
   *
   * @return the tag value
   */
  def getTagValue(tagList: NodeList, tagName: String): Option[String] = {
    for (index <- 0 until tagList.getLength) {
      val element = tagList.item(index).asInstanceOf[Element]
      val attribute = element.getAttributeNode("TagName").getValue
      if (attribute.equals(tagName)) {
        val tagValue = element.getAttributeNode("TagValue")
          .getValue
          .toLowerCase
          .trim
          .replaceAll("\\s{2,}", " ")

        return Some(tagValue)
      }
    }

    None
  }

  /**
   * Gets the message lines from the corresponding text file.
   *
   * @param fileList the file elements as a NodeList
   *
   * @return an array of lines
   */
  def getMessageWordCount(fileList: NodeList, zip: ZipFile): Int = {
    var textFilePath: String = null
    breakable {
      for (index <- 0 until fileList.getLength) {
        val element = fileList.item(index).asInstanceOf[Element]
        val attribute = element.getAttributeNode("FileType").getValue
        if (attribute.equals("Text")) {
          val externalFile = element.getElementsByTagName("ExternalFile")
          val filePath = externalFile.item(0).asInstanceOf[Element].getAttributeNode("FilePath").getValue
          val fileName = externalFile.item(0).asInstanceOf[Element].getAttributeNode("FileName").getValue
          textFilePath = filePath.concat("/").concat(fileName)
          break
        }
      }
    }

    if (textFilePath == null) {
      return 0
    }

    val inputStream = zip.getInputStream(zip.getEntry(textFilePath))
    val br = new BufferedReader(new InputStreamReader(inputStream));
    val lines = br.lines.toArray()
    val msgHeaders = new Regex("((Date|Message-ID|MIME-Version|" +
      "Content-Type|From|To|Subject|Content-Transfer-Encoding|" +
      "X-Filename|X-Folder|X-SDOC|X-ZLID):.+|^\\s*$)")
    
    val msgWordCount = lines.filterNot(l => msgHeaders.findAllIn(l.toString).length == 0)
      .map(l => removePunctuation(l.toString.trim))
      .map(_.split("\\s"))
      .map(_.length)
      .sum

    return msgWordCount

  }

  /**
   * Clean out punctuation from the input.
   *
   * @param target the string to clean
   * @return the output of the replacement
   */
  def removePunctuation(target: String): String = {
    val regex = new Regex("\\p{Punct}")
    regex.replaceAllIn(target, "")
  }

}
