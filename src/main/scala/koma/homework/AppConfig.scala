package koma.homework

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.scala.Serdes

object AppConfig {

  def props: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG            , "homework-v0.2")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG         , "localhost:9092")
    p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG   , Serdes.String.getClass)
    p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG , Serdes.Integer.getClass)
    p.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG        , 1)
    p.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG     , 500)
    p
  }

}
