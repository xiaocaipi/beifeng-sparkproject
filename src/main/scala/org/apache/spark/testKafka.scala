package org.apache.spark

import org.apache.spark.streaming.kafka.KafkaCluster

/**
  * Created by caidanfeng733 on 8/21/17.
  */
object testKafka {

  def main(args: Array[String]) {


    val groupId = "testkafka_group"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> "cdh1:9092",
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
    )
    val kc = new KafkaCluster(kafkaParams)


    val partitionsE = kc.getPartitions(Set("AdRealTimeLog"))
    val partitions = partitionsE.right.get

    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
    if(consumerOffsetsE.isLeft){
      val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
      println(earliestLeaderOffsetsE)
    }

    if(consumerOffsetsE.isRight){
      val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
      val leaderOffsets= leaderOffsetsE.right.get

      val offsets = leaderOffsets.map {
        case (tp, offset) => (tp, offset.offset)
      }
      print(offsets)
    }


//    println(leaderOffsetsE)
  }
}
