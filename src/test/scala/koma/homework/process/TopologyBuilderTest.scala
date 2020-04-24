package koma.homework.process


import koma.homework.AppConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.scalatest.flatspec.AnyFlatSpec

import scala.io.Source


class TopologyBuilderTest extends AnyFlatSpec {

  val stringDeserializer = new StringDeserializer()
  val intDeserializer = new IntegerDeserializer()

  private def fixture = {
    val builder = new StreamsBuilder()
    val source = builder.stream[String, String]("homework")
    val result = TopologyBuilder.prepareTopology(source)
    result.to("homework-output")
    new {
      val testDriver = new TopologyTestDriver(builder.build(), AppConfig.props)
      val factory = new ConsumerRecordFactory("homework", new IntegerSerializer(), new StringSerializer())
    }
  }

  "Confirmation event" should "be processed correctly" in {
    val f = fixture
    val input = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
    f.testDriver.pipeInput(f.factory.create(null.asInstanceOf[Integer], input))
    val result = outputToMap(f.testDriver)
    assert(result.get("TyazVPlL11HYaTGs1_dc1").contains(1), "Number of confirmed segments is not correct")
    f.testDriver.close()
  }

  "Confirmation and de-confirmation events" should "be processed correctly" in {
    val f = fixture
    val input1 = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
    f.testDriver.pipeInput(f.factory.create(null.asInstanceOf[Integer], input1))
    val input2 = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 0,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
    f.testDriver.pipeInput(f.factory.create(null.asInstanceOf[Integer], input2))
    val result = outputToMap(f.testDriver)
    assert(result.get("TyazVPlL11HYaTGs1_dc1").contains(0), "Number of confirmed segments is not correct")
    f.testDriver.close()
  }

  "Repeated confirmation event" should "increase the number of segments once" in {
    val f = fixture
    val input = "{\"after\":\"{\\\"_id\\\" : {\\\"$oid\\\" : \\\"5cab7448afffbd5cf98cac46\\\"},\\\"taskId\\\" : \\\"TyazVPlL11HYaTGs1_dc1\\\",\\\"tGroupId\\\" : 1,\\\"levels\\\" : [1],\\\"createdAt\\\" : {\\\"$numberLong\\\" : \\\"1554740296760\\\"},\\\"tUnits\\\" : [{\\\"tUnitId\\\" : \\\"TyazVPlL11HYaTGs1_dc1:1\\\",\\\"src\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"target\\\" : \\\"{2>The Great Charter of English liberty granted (under considerable duress) by King John at Runnymede on June 15, 1215<2}\\\",\\\"confirmedLevel\\\" : 1,\\\"locked\\\" : false,\\\"score\\\" : 0.0,\\\"grossScore\\\" : 0.0,\\\"roboTrans\\\" : \\\"\\\",\\\"roboScore\\\" : 0.0,\\\"transOriginDetail\\\" : \\\"null\\\",\\\"roboTransOriginDetail\\\" : \\\"nt_ai\\\",\\\"machineTrans\\\" : \\\"\\\",\\\"machineTransScore\\\" : 0.0,\\\"bestTmTrans\\\" : \\\"\\\",\\\"bestTMScore\\\" : 0.0,\\\"transOrigin\\\" : \\\"null\\\",\\\"createdAt\\\" : \\\"1554740266677\\\",\\\"createdBy\\\" : \\\"764\\\",\\\"modifiedAt\\\" : \\\"1554740295902\\\",\\\"modifiedBy\\\" : \\\"764\\\",\\\"qaDatas\\\" : [{\\\"level\\\" : 1,\\\"qaProcessed\\\" : false,\\\"qaIgnoredChecks\\\" : [\\\"Spellcheck:considerable\\\", \\\"Spellcheck:of\\\", \\\"Spellcheck:duress\\\", \\\"Spellcheck:English\\\", \\\"Spellcheck:granted\\\", \\\"Spellcheck:The\\\", \\\"Spellcheck:liberty\\\", \\\"Spellcheck:under\\\", \\\"Spellcheck:at\\\", \\\"Spellcheck:Runnymede\\\"]}],\\\"marksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"targetMarksData\\\" : [{\\\"id\\\" : \\\"2\\\",\\\"content\\\" : \\\"<w:r><w:rPr><w:rFonts></w:rFonts><w:b w:val=\\\\\\\"false\\\\\\\"></w:b><w:i></w:i><w:caps w:val=\\\\\\\"false\\\\\\\"></w:caps><w:smallCaps w:val=\\\\\\\"false\\\\\\\"></w:smallCaps><w:strike w:val=\\\\\\\"false\\\\\\\"></w:strike><w:dstrike w:val=\\\\\\\"false\\\\\\\"></w:dstrike><w:color w:val=\\\\\\\"444333\\\\\\\"></w:color><w:spacing w:val=\\\\\\\"0\\\\\\\"></w:spacing><w:sz w:val=\\\\\\\"16\\\\\\\"></w:sz><w:u w:val=\\\\\\\"none\\\\\\\"></w:u><w:effect w:val=\\\\\\\"none\\\\\\\"></w:effect></w:rPr><w:t></w:t></w:r>\\\"}],\\\"levelEditingStats\\\" : {\\\"1\\\" : \\\"<m:editing-stats xmlns:m=\\\\\\\"http://www.memsource.com/mxlf/2.0\\\\\\\">\\\\r\\\\n<m:editing-time>129689</m:editing-time>\\\\r\\\\n<m:thinking-time>104033</m:thinking-time>\\\\r\\\\n</m:editing-stats>\\\"}}],\\\"paraId\\\" : \\\"0\\\",\\\"context\\\" : {\\\"filePart\\\" : \\\"word/document.xml::body\\\"},\\\"actionId\\\" : \\\"uo4pufh4le90\\\"}\",\"patch\":null,\"source\":{\"version\":\"0.8.1.Final\",\"name\":\"qa-debezium\",\"rs\":\"qa_set\",\"ns\":\"converter.tgroup\",\"sec\":1554740296,\"ord\":5,\"h\":1914709261723006611,\"initsync\":false},\"op\":\"c\",\"ts_ms\":1554740296776}\n"
    0 to 2 foreach( _ => f.testDriver.pipeInput(f.factory.create(null.asInstanceOf[Integer], input)) )
    val result = outputToMap(f.testDriver)
    assert(result.get("TyazVPlL11HYaTGs1_dc1").contains(1), "Number of confirmed segments is not correct")
    f.testDriver.close()
  }

  "Events from the sample input file" should "be processed correctly" in {
    val f = fixture
    pipeFileToDriver(f.testDriver, f.factory)
    val result = outputToMap(f.testDriver)
    assert(result.get("jNazVPlL11HFhTGs1_dc1").contains(0), "Number of confirmed segments is not correct")
    assert(result.get("TyazVPlL11HYaTGs1_dc1").contains(2), "Number of confirmed segments is not correct")
    f.testDriver.close()
  }

  "Events from the sample input file" should "be processed correctly when piped repeatedly" in {
    val f = fixture
    0 to 2 foreach( _ => pipeFileToDriver(f.testDriver, f.factory) )
    val result = outputToMap(f.testDriver)
    assert(result.get("jNazVPlL11HFhTGs1_dc1").contains(0), "Number of confirmed segments is not correct")
    assert(result.get("TyazVPlL11HYaTGs1_dc1").contains(2), "Number of confirmed segments is not correct")
    f.testDriver.close()
  }

  private def outputToMap(testDriver: TopologyTestDriver): Map[String, Int] = {
    var result: Map[String, Int] = Map()
    var out: ProducerRecord[String, Integer] = null
    do {
      out = testDriver.readOutput[String, Integer]("homework-output", stringDeserializer, intDeserializer)
      if (out != null) result += out.key -> out.value
    } while (out != null)
    result
  }

  private def pipeFileToDriver(testDriver: TopologyTestDriver, factory: ConsumerRecordFactory[Integer, String]) = {
    val stream = getClass.getResourceAsStream("/kafka-messages.jsonline")
    Source.fromInputStream(stream)
      .getLines
      .filter(!_.isEmpty)
      .foreach(line => testDriver.pipeInput(factory.create(null.asInstanceOf[Integer], line)))
  }

}
