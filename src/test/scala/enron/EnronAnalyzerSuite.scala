package enron

import org.scalatest._

class EnronAnalyzerSuite extends FunSuite with BeforeAndAfterAll {

  val emails = Array(
    EmailMessage(
      Some("someone@example.com, unknown, somebody@example.com, mailing list"),
      Some("someone2@example.com"),
      Some(List("headers", "this is line 1", "  ", "regards", "end of message"))),
    EmailMessage(
      Some("someone@example.com, unknown, someone2@example.com, mailing list"),
      Some("somebody@example.com"),
      Some(List("headers", "this is line 1", "  ", "regards", "end of message"))),
    EmailMessage(
      Some("someone@example.com, unknown, somebody@example.com, mailing list"),
      None,
      Some(List("headers", "hello world", "  ", "regards", "end of message"))))

  override def afterAll(): Unit = {
    import EnronAnalyzer._
    sc.stop()
  }

  test("recipientCount should return the correct email address counts of the given messages") {
    import EnronAnalyzer._
    val rc = EnronAnalyzer.recipientCount(emails).sortBy(-_._2)
    assert(rc.count() == 3, "recipientCount should return an RDD of the correct length")

    // test table of expected recipient counts
    val tt = List(("someone@example.com", 3.0), ("somebody@example.com", 2.5), ("someone2@example.com", 1.5))
    val res = rc.collect

    for (i <- 0 until res.length) {
      assert(res(i)._1 == tt(i)._1, "recipientCount should return the correct email address")
      assert(res(i)._2 == tt(i)._2, "recipientCount should return the correct score for the email address")
    }
  }

  test("averageMsgWordCount should return the correct word count average of the given messages") {
    import EnronAnalyzer._
    val avgWc = EnronAnalyzer.averageMsgWordCount(emails)
    assert(avgWc.toInt == 8, "recipientCount should return the correct word count average")
  }

  test("topRecipients should order the top number of recipients by their score in descending order") {
    import EnronAnalyzer._
    val rc = sc.parallelize(List(
      ("e0@example.com", 2.5),
      ("e1@example.com", 4.5),
      ("e2@example.com", 1.0),
      ("e3@example.com", 8.0),
      ("e4@example.com", 8.5),
      ("e5@example.com", 7.0),
      ("e6@example.com", 9.0)))

    val tr = EnronAnalyzer.topRecipients(rc, 5)

    // test table of expected ordering of recipients
    val tt = List(
      ("e6@example.com", 9.0),
      ("e4@example.com", 8.5),
      ("e3@example.com", 8.0),
      ("e5@example.com", 7.0),
      ("e1@example.com", 4.5))
    
    for (i <- 0 until tr.length) {
      assert(tr(i)._1 == tt(i)._1, "topRecipients should correctly order the email addresses")
      assert(tr(i)._2 == tt(i)._2, "topRecipients should correctly order the email addresses")
    }
  }

}