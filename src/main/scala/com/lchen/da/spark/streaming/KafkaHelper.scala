package com.lchen.da.spark.streaming

import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.{KafkaCluster, OffsetRange}
import org.slf4j.{Logger, LoggerFactory}

/**
  * kafka offset helper
  *
  * get or commit offsets to kafka, not zookeeper
  *
  * metadata.broker.list       -- required
  * auto.offset.reset          -- optional, default latest
  * auto.create.topics.enable  -- optional, default true
  * enable.auto.commit         -- optional, default true but set false now
  *
  * Created by hzchenlei1.
  */
class KafkaHelper(kafkaParams:Map[String, String]) {

    val logger: Logger = LoggerFactory.getLogger(classOf[KafkaHelper])
    val kc = new KafkaCluster(kafkaParams)

    def getFromOffsets(topics:Set[String], groupId:String): Map[TopicAndPartition, Long] ={
        if(null==topics || topics.isEmpty)  return null
        if(null==groupId || "".equals(groupId)) return null

        // 1. get all topics and partitions
        val result = kc.getPartitions(topics)

        val all_tps:Set[TopicAndPartition] = result match {
            case `result` if result.isLeft => logger.error("Get partitions error!"); return null
            case `result` if result.right.get.isEmpty => logger.warn("No partition was found!"); return null
            case _ => result.right.get
        }

        if(null==all_tps) return null

        // 2. get stored offsets from kafka cluster
        val stored_tps_offsets = kc.getConsumerOffsets(groupId, all_tps)

        val tps_startOffsets = if(stored_tps_offsets.isLeft){  // 2.1  new group id
            createStartOffsetsForNewGroup(all_tps, groupId)
        }
        else {  // 2.2  exist group id
            createStartOffsetsForExistGroup(stored_tps_offsets.right.get, all_tps, groupId)
        }

        tps_startOffsets
    }

    def setConsumerOffsets(groupId:String, offsetRanges:Array[OffsetRange]): Unit ={
        var tps = Map[TopicAndPartition, Long]()
        for(range <- offsetRanges){
            tps += range.topicAndPartition() -> range.untilOffset
        }
        setConsumerOffsets(groupId, tps)
    }

    def setConsumerOffsets(groupId:String, tps:Map[TopicAndPartition, Long]): Unit ={
        kc.setConsumerOffsets(groupId, tps)
    }

    private def createStartOffsetsForNewGroup(tps:Set[TopicAndPartition], groupId:String): Map[TopicAndPartition, Long] ={
        logger.info("Create start offsets for a new group: "+groupId)
        val startFlag = kafkaParams.getOrElse("auto.offset.reset", "latest").toLowerCase
        getLeaderOffset(tps, startFlag)
    }

    private def createStartOffsetsForExistGroup(storedTPOffsets:Map[TopicAndPartition, Long],
                                                tps:Set[TopicAndPartition], groupId:String): Map[TopicAndPartition, Long] ={
        logger.info("Create start offsets for a exist group: "+groupId)

        val startFlag = kafkaParams.getOrElse("auto.offset.reset", "latest").toLowerCase
        var tps_startOffset = Map[TopicAndPartition, Long]()

        for(tp <- tps){
            if(storedTPOffsets.contains(tp)){
                // case 1: judge if this stored offset has expired
                val range = getLeaderOffsetRange(Set(tp))
                val storedOffset = storedTPOffsets.apply(tp)
                if(storedOffset >= range.apply(tp)._1){
                    // valid offset
                    tps_startOffset += (tp -> storedOffset)
                }else{
                    // invalid expired offset
                    logger.warn("Found a invalid expired partition offset: {} and auto.offset.reset：{}", tp.partition ,startFlag)
                    if("earliest".equals(startFlag))
                        tps_startOffset += (tp -> range.apply(tp)._1)
                    else
                        tps_startOffset += (tp -> range.apply(tp)._2)
                }
            }else{
                // case 2: create a new start offset for new topic partition( May be some topic had added new partitions.)
                logger.warn("Found a new partition in a exist topic: {} and auto.offset.reset：{}", tp.partition ,startFlag)
                tps_startOffset ++= getLeaderOffset(Set(tp), startFlag)
            }
        }

        tps_startOffset
    }

    private def getLeaderOffset(tps:Set[TopicAndPartition], autoOffsetReset:String): Map[TopicAndPartition, Long] ={
        val leaderOffsetRanges = getLeaderOffsetRange(tps)
        if("earliest".equals(autoOffsetReset.toLowerCase()))
            for (range <- leaderOffsetRanges) yield range._1 -> range._2._1
        else
            for (range <- leaderOffsetRanges) yield range._1 -> range._2._2
    }

    /*
     * return offset range of every leader partition
     *  (tp -> (earliestOffset, latestOffset))
     */
    private def getLeaderOffsetRange(tps:Set[TopicAndPartition]): Map[TopicAndPartition, (Long, Long)] ={
        val minOffsets = kc.getEarliestLeaderOffsets(tps).right.get
        val maxOffsets = kc.getLatestLeaderOffsets(tps).right.get

        var offsetRange = Map[TopicAndPartition, (Long, Long)]()
        for(tp <- tps){
            offsetRange += (tp -> (minOffsets.apply(tp).offset, maxOffsets.apply(tp).offset))
        }
        offsetRange
    }

}