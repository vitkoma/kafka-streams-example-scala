package koma.homework.process

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import koma.homework.model.{ModEvent, OpType, TGroup}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._


/**
 * Defines the topology of data transformation from a source stream to a result stream.
 */
object TopologyBuilder {

  private val jsonMapper = new ObjectMapper
  jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  jsonMapper.registerModule(DefaultScalaModule)

  def prepareTopology(source: KStream[String, String]): KStream[String, Int] =
    source
      .mapValues ( modJson => jsonMapper.readValue(modJson, classOf[ModEvent]))
      .filter ( (_, modEvent) => modEvent.op == OpType.Create)
      .mapValues ( modEvent => jsonMapper.readValue(modEvent.after, classOf[TGroup]))
      .filter ( (_, tGroup) => tGroup.levels != null && tGroup.levels.nonEmpty)
      .selectKey ( (_, tGroup) => tGroup.tUnits(0).tUnitId)
      .mapValues ( tGroup => if (tGroupConfirmed(tGroup)) 1 else 0)
      .groupByKey // group by segment ID, the last event's confirmation status is the relevant one for the given segment
      .reduce ( (_, newValue) => newValue)
      .groupBy ( (tUnitId, confirmed) => Tuple2(tUnitId.substring(0, tUnitId.indexOf(':')), confirmed)) // group by task ID and sum the statuses
      .reduce (
        (aggValue, newValue) => aggValue + newValue,
        (aggValue, oldValue) => aggValue - oldValue)
      .toStream

  private def tGroupConfirmed(tGroup: TGroup) = tGroup.tUnits(0).confirmedLevel == tGroup.levels(0)

}
