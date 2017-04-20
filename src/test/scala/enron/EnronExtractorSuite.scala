package enron

import org.scalatest._

import org.w3c.dom._
import javax.xml.parsers.{ DocumentBuilderFactory, DocumentBuilder }
import java.io.{ InputStream, File }
import java.util.zip.ZipFile

class EnronExtractorSuite extends FunSuite with BeforeAndAfterAll {

  var testZipFile: ZipFile = null
  var docList: NodeList = null

  override def beforeAll(): Unit = {

    val file = new File(this.getClass.getClassLoader.getResource("test.xml").toURI.getPath)
    val dBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder()
    val testXml: Document = dBuilder.parse(file)
    testZipFile = new ZipFile(this.getClass.getClassLoader.getResource("test.zip").toURI.getPath)

    // docList represents all Document elements of the test XML
    docList = testXml.getElementsByTagName("Document")
  }

  test("getTagValue should return a tag value for a present tag name") {
    val firstDoc = docList.item(0).asInstanceOf[Element]
    val tagList = firstDoc.getElementsByTagName("Tag")
    val to = EnronExtractor.getTagValue(tagList, "#To")
    assert(to != None, "getTagValue should return some tag value")
    assert(to.get == "someone@example.com", "getTagValue should return the correct value")
  }

  test("getTagValue should return nothing for tag name that is not present") {
    val firstDoc = docList.item(0).asInstanceOf[Element]
    val tagList = firstDoc.getElementsByTagName("Tag")
    val to = EnronExtractor.getTagValue(tagList, "not exist")
    assert(to == None, "getTagValue should return None")
  }

  test("getMessageLines should return the lines of the corresponding email") {
    val firstDoc = docList.item(0).asInstanceOf[Element]
    val fileList = firstDoc.getElementsByTagName("File")

    val mls = EnronExtractor.getMessageLines(fileList, testZipFile)
    assert(mls != None, "getMessageLines should return some list of lines")
    assert(mls.get.length == 12, "should be 12")
    val msgs = mls.get
    assert(msgs(0).equals("Date: Tue, 30 May 2000 10:34:00 -0700 (PDT)"), "getMessageLines "
      + "should return the correct line")

    assert(msgs(5).matches("""^\s*$"""), "getMessageLines should "
      + "return the correct line")

    assert(msgs(6).equals("This is just a test message."), "getMessageLines should "
      + "return the correct line")

    assert(msgs(11).equals("aleonard@caiso.com"), "getMessageLines should return the correct line")
  }

  test("extractEmailMessagesFromXml should return the primary and carbon copy recipients, "
    + "and message lines or the emails") {

    val ems = EnronExtractor.extractEmailMessagesFromXml(docList, testZipFile)
    assert(ems.length == 3, "extractEmailMessagesFromXml should return the correct number of email messages")

    // expected email assertions:
    // - check type returned for primary recipient
    // - check type returned for carbon copy recipient
    // - check number of message lines
    val e0 = (true, false, 12)
    val e1 = (true, true, 19)
    val e2 = (true, true, 18)

    // test table
    val tt = List(e0, e1, e2)

    for (i <- 0 until ems.length) {
      assert(ems(i).to.isInstanceOf[Some[String]] == tt(i)._1, "extractEmailMessagesFromXml "
        + "should return the correct type for primary recipients")

      assert(ems(i).cc.isInstanceOf[Some[String]] == tt(i)._2, "extractEmailMessagesFromXml "
        + "should return the correct type for carbon copy recipient")

      assert(ems(i).lines.get.length == tt(i)._3, "extractEmailMessagesFromXml "
        + "should return the correct number of message lines")
    }

  }

  test("extractEmailMessagesFromZip should return the emails belonging to the Zip") {
    val ems = EnronExtractor.extractEmailMessagesFromZip(testZipFile)
    assert(ems != None, "extractEmailMessagesFromZip should have returned some list of lines")
    assert(ems.get.length == 3, "should be 3")

    // expected email assertions:
    // - check type returned for primary recipient
    // - check type returned for carbon copy recipient
    // - check number of message lines
    val e0 = (true, false, 12)
    val e1 = (true, true, 19)
    val e2 = (true, true, 18)

    // test table
    val tt = List(e0, e1, e2)

    for (i <- 0 until ems.get.length) {
      assert((ems.get)(i).to.isInstanceOf[Some[String]] == tt(i)._1, "extractEmailMessagesFromZip "
        + "should return the correct type for primary recipients")

      assert((ems.get)(i).cc.isInstanceOf[Some[String]] == tt(i)._2, "extractEmailMessagesFromZip "
        + "should return the correct type for carbon copy recipient")

      assert((ems.get)(i).lines.get.length == tt(i)._3, "extractEmailMessagesFromZip "
        + "should return the correct number of message lines")
    }
  }

}