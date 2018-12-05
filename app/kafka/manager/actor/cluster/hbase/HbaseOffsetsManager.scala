package kafka.manager.actor.cluster.hbase

import java.text.SimpleDateFormat
import java.util.Properties

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import grizzled.slf4j.Logging
import kafka.manager.actor.cluster.hbase.HbaseOffsetsManager.config
import org.apache.hadoop.hbase.client.{Delete, Put, Scan, Table}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.kafka.clients.consumer.ConsumerConfig._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by guoning on 2018/12/3.
  *
  */
object HbaseOffsetsManager extends Logging {


  implicit lazy val formats = org.json4s.DefaultFormats

  private lazy val config = ConfigFactory.load().getConfig("offset.store")

  val storeParams = config.entrySet().map(x => x.getKey -> config.getString(x.getKey)).toMap

  private lazy val tableName = storeParams("hbase.table")
  private lazy val familyName = storeParams("hbase.family")
  private lazy val familyNameBytes = Bytes.toBytes(familyName)
  private lazy val topicBytes = Bytes.toBytes("topic")
  private lazy val partitionBytes = Bytes.toBytes("partition")
  private lazy val offsetBytes = Bytes.toBytes("offset")


  @transient
  private lazy val table: Table = {
    val t = HbaseConnPool.connect(storeParams).getTable(TableName.valueOf(tableName))
    logger.info(s"get hbase table $tableName $t")
    t
  }


  /** 存放offset的表模型如下，请自行优化和扩展，请把每个rowkey对应的record的version设置为1（默认值），因为要覆盖原来保存的offset，而不是产生多个版本
    * ----------------------------------------------------------------------------------------------------
    * rowkey                    |  column family                                                          |
    * ----------------------------------------------------------------------------------------------------
    * |                         |  column:topic(string)  |  column:partition(int)  | column:offset(long)  |
    * ----------------------------------------------------------------------------------------------
    * groupId#topic#partition   |   topic                |   partition             |    offset            |
    * ---------------------------------------------------------------------------------------------------
    */


  /**
    * 获取消费组的消费信息
    *
    * @param consumer
    * @param topic
    * @return
    */
  def getConsumerOffset(consumer: String, topic: String): Map[Int, Long] = {
    getOffsets(consumer, Set(topic)).map(x => x._1.partition -> x._2)
  }


  /**
    * 获取所有消费组
    *
    * @return
    */
  def getConsumers: Set[String] = {
    val filter = new KeyOnlyFilter()
    val scan = new Scan().setFilter(filter)

    val rs = table.getScanner(scan)
    val r = rs.asScala.map(r => Bytes.toString(r.getRow).split("#").head).toSet
    rs.close()
    r
  }

  /**
    * 获取消费组下的所有Topic
    *
    * @param consumer
    * @return
    */
  def getTopics(consumer: String): Set[String] = {

    val filterAnd = new FilterList(FilterList.Operator.MUST_PASS_ALL)

    val keyOnly = new KeyOnlyFilter()

    val prefixFilter = new PrefixFilter(Bytes.toBytes(consumer))
    filterAnd.addFilter(keyOnly)
    filterAnd.addFilter(prefixFilter)

    val scan = new Scan().setFilter(filterAnd)

    val rs = table.getScanner(scan)
    val r = rs.asScala.map(r => Bytes.toString(r.getRow)).filter(_.startsWith(consumer)).map(_.split("#")(1)).toSet
    rs.close()
    r
  }


  def updateOffsetForTime(broker: String, groupId: String, topic: String, time: String): Unit = {

    logger.info(s"updateOffsetForTime broker [$broker] groupId [$groupId] topic [$topic] time [$time]")

    val props: Properties = new Properties()
    props.put(GROUP_ID_CONFIG, s"KSGetOffsetForTime")
    props.put(BOOTSTRAP_SERVERS_CONFIG, broker)
    props.put(EXCLUDE_INTERNAL_TOPICS_CONFIG, "false")
    props.put(ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaConsumer = new KafkaConsumer(props)

    val partitions = kafkaConsumer.partitionsFor(topic).asScala

    val topicPartition = partitions.map { p => new TopicPartition(p.topic(), p.partition()) }

    def javaLongToLong(l: java.lang.Long): Long = l + 0L

    val offsetInfosOpt = time.toLowerCase match {
      case "latest" =>
        val offset = kafkaConsumer.endOffsets(topicPartition.asJava)
        Some(offset.mapValues(javaLongToLong))
      case "earliest" =>
        val offset = kafkaConsumer.beginningOffsets(topicPartition.asJava)
        Some(offset.mapValues(javaLongToLong))
      case _ => //None
        Try {
          val t = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(time).getTime
          val timestampsToSearch = topicPartition map (p => p -> java.lang.Long.valueOf(t)) toMap

          kafkaConsumer.offsetsForTimes(timestampsToSearch.asJava).map { case (tp, o) => tp -> javaLongToLong(o.offset()) }

        } match {
          case Success(offset) => Some(offset)
          case Failure(e) =>
            logger.warn(s"illegal fields time [$time]")
            e.printStackTrace()
            None
        }
    }

    offsetInfosOpt.foreach { offsetInfos =>
      kafkaConsumer.subscribe(Set(topic).asJava)
      updateOffsets(groupId, offsetInfos.toMap)
    }


    kafkaConsumer.close()
  }

  /**
    * 获取存储的Offset
    *
    * @param groupId
    * @param topics
    * @return
    */
  def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    val storedOffsetMap = new mutable.HashMap[TopicPartition, Long]()

    for (topic <- topics) {
      val key = generateKey(groupId, topic)
      val filter = new PrefixFilter(key.getBytes)
      val scan = new Scan().setFilter(filter)
      val rs = table.getScanner(scan)
      rs.asScala.map(r => {
        val cells = r.rawCells()

        var topic = ""
        var partition = 0
        var offset = 0L

        cells.foreach(cell => {
          Bytes.toString(CellUtil.cloneQualifier(cell)) match {
            case "topic" => topic = Bytes.toString(CellUtil.cloneValue(cell))
            case "partition" => partition = Bytes.toInt(CellUtil.cloneValue(cell))
            case "offset" => offset = Bytes.toLong(CellUtil.cloneValue(cell))
            case other =>
          }
        })

        val tp = new TopicPartition(topic, partition)

        storedOffsetMap += tp -> offset
      })
      rs.close()
    }
    storedOffsetMap.toMap
  }

  /**
    * 更新 Offsets
    *
    * @param groupId
    * @param offsetInfos
    */
  def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {
    val puts = offsetInfos.map {
      case (tp, offset) =>
        val put: Put = new Put(Bytes.toBytes(s"${generateKey(groupId, tp.topic)}#${tp.partition}"))
        put.addColumn(familyNameBytes, topicBytes, Bytes.toBytes(tp.topic))
        put.addColumn(familyNameBytes, partitionBytes, Bytes.toBytes(tp.partition))
        put.addColumn(familyNameBytes, offsetBytes, Bytes.toBytes(offset))
        put
    } toList

    table.put(puts.asJava)
    logger.info(s"updateOffsets [ $groupId,${offsetInfos.mkString(",")} ]")
  }

  /**
    * 删除Offset
    *
    * @param groupId
    * @param topics
    */
  def delOffsets(groupId: String, topics: Set[String]): Unit = {

    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE)

    for (topic <- topics) {
      val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes(s"${generateKey(groupId, topic)}#")))
      filterList.addFilter(filter)
    }

    val scan = new Scan()
    scan.setFilter(filterList)

    val rs = table.getScanner(scan)
    val iter = rs.iterator()

    import java.util

    val deletes = new util.ArrayList[Delete]()
    while (iter.hasNext) {
      val r = iter.next()
      deletes.add(new Delete(Bytes.toBytes(new String(r.getRow))))
    }
    rs.close()
    table.delete(deletes)
    logger.info(s"deleteOffsets [ $groupId,${topics.mkString(",")} ] ${deletes.asScala.mkString(" ")}")
  }


  /**
    * 生成Key
    *
    * @param groupId
    * @param topic
    * @return
    */
  def generateKey(groupId: String, topic: String): String = s"$groupId#$topic"

}
