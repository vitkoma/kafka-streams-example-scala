package koma.homework

import java.util.concurrent.CountDownLatch

import koma.homework.process.TopologyBuilder
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.Printed
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

/**
 * The application entry point.
 * Based on the original skeleton.
 */
object Application extends App {

  val builder = new StreamsBuilder

  val source = builder.stream[String, String]("homework")
  source.print(Printed.toSysOut[String, String])
  // your code can start here

  val result = TopologyBuilder.prepareTopology(source)
  result.print(Printed.toSysOut[String, Int])
  result.to("homework-output")

  // end
  val topology = builder.build
  System.out.println(topology.describe)
  val streams = new KafkaStreams(topology, AppConfig.props)
  val latch = new CountDownLatch(1)

  // attach shutdown handler to catch control-c
  sys.ShutdownHookThread {
    streams.close()
    latch.countDown()
  }

  try {
    streams.start()
    latch.await()
  } catch {
    case _: Throwable =>
      System.exit(1)
  }
  System.exit(0)

}
