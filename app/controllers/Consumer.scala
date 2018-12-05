/**
  * Copyright 2015 Yahoo Inc. Licensed under the Apache License, Version 2.0
  * See accompanying LICENSE file.
  */

package controllers

import features.ApplicationFeatures
import kafka.manager.BrokerListExtended
import kafka.manager.actor.cluster.hbase.HbaseOffsetsManager
import kafka.manager.model.ConsumerTuning.UpdateOffsetForTime
import models.navigation.Menus
import play.api.data.Form
import play.api.data.Forms._
import play.api.i18n.{I18nSupport, MessagesApi}
import play.api.mvc._
import grizzled.slf4j.Logging
import kafka.manager.jmx.KafkaJMX.logger

/**
  * @author cvcal
  */
class Consumer(val messagesApi: MessagesApi, val kafkaManagerContext: KafkaManagerContext)
              (implicit af: ApplicationFeatures, menus: Menus) extends Controller with I18nSupport with Logging {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  private[this] val kafkaManager = kafkaManagerContext.getKafkaManager

  def consumers(cluster: String) = Action.async {
    kafkaManager.getConsumerListExtended(cluster).map { errorOrConsumerList =>
      Ok(views.html.consumer.consumerList(cluster, errorOrConsumerList))
    }
  }

  def consumer(cluster: String, consumerGroup: String, consumerType: String) = Action.async {
    kafkaManager.getConsumerIdentity(cluster, consumerGroup, consumerType).map { errorOrConsumerIdentity =>
      Ok(views.html.consumer.consumerView(cluster, consumerGroup, errorOrConsumerIdentity))
    }
  }

  def consumerAndTopic(cluster: String, consumerGroup: String, topic: String, consumerType: String) = Action.async {
    kafkaManager.getConsumedTopicState(cluster, consumerGroup, topic, consumerType).map { errorOrConsumedTopicState =>
      Ok(views.html.consumer.consumedTopicView(cluster, consumerGroup, consumerType, topic, errorOrConsumedTopicState))
    }
  }


  def brokerListExtendedToBrokerList(brokerList: BrokerListExtended): String = {
    brokerList.list.map(x => s"${x.host}:${x.endpoints.values.head}").mkString(",")
  }

  def updateOffsetForTime(cluster: String) = Action.async { implicit request =>

    updateOffsetForTimeForm.bindFromRequest().value.map { x =>
      kafkaManager.getBrokerList(cluster).map { errorOrBrokerList =>
        errorOrBrokerList.fold(
          erroe => logger.error(s"获取 【$cluster】brokerList 异常 "),
          brokerList => HbaseOffsetsManager.updateOffsetForTime(brokerListExtendedToBrokerList(brokerList), x.consumer, x.topic, x.time)
        )
      }
      kafkaManager.getConsumedTopicState(cluster, x.consumer, x.topic, x.consumerType).map { errorOrConsumedTopicState =>
        Ok(views.html.consumer.consumedTopicView(cluster, x.consumer, x.consumerType, x.topic, errorOrConsumedTopicState, x.time))
      }
    } get
  }

  import play.api.data.format.Formats._

  val updateOffsetForTimeForm = Form(
    mapping(
      "consumer" -> of[String],
      "topic" -> of[String],
      "consumerType" -> of[String],
      "time" -> of[String]
    )(UpdateOffsetForTime.apply)(UpdateOffsetForTime.unapply))
}
