package cmgd.zenghj.hss.actor

import akka.actor.{Props, ActorRef}
import akka.routing.{RoundRobinPool, DefaultResizer}
import cmgd.zenghj.hss.common.CommonUtils._

import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import cmgd.zenghj.hss.redis.RedisUtils._
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import com.softwaremill.react.kafka.{ConsumerProperties, ReactiveKafka}
import com.twitter.chill.KryoInjection
import kafka.serializer.DefaultDecoder
import org.reactivestreams.Publisher

import scala.util.{Failure, Success, Try}

/**
  * Created by cookeem on 16/7/31.
  */

class GetFileWorker extends  TraitClusterActor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  val resizer = DefaultResizer(lowerBound = getFileWorkerStreamCount, upperBound = getFileWorkerStreamCount * 2)
  val ftpKafkaRouter: ActorRef = context.actorOf(RoundRobinPool(getFileWorkerStreamCount, Some(resizer)).props(Props[FtpKafkaWorker]), "ftpkafka-router")

  var fileCount = 0
  var recordCount = 0
  var fileFailCount = 0

  //启动从kafka读取文件列表, 下载ftp文件后,把文件记录入库到kafka的reactive kafka stream
  ftpToKafkaStream()

  //从kafka读取文件列表, 下载ftp文件后,把文件记录入库到kafka
  def ftpToKafkaStream(): NotUsed = {
    val kafka = new ReactiveKafka()
    //从kafka-files-topic获取最新的文件
    val publisher: Publisher[KafkaMessage[Array[Byte]]] = kafka.consume(ConsumerProperties(
      brokerList = kafkaBrokers,
      zooKeeperHost = kafkaZkUri,
      topic = kafkaFilesTopic,
      groupId = kafkaConsumeGroup,
      decoder = new DefaultDecoder()
    ))
    Source.fromPublisher(publisher).map { kafkaMsg =>
      val tryObj = KryoInjection.invert(kafkaMsg.message)
      tryObj match {
        case Success(obj) =>
          val tryObj2 = Try(obj.asInstanceOf[(String, String)])
          tryObj2 match {
            case Success((dir, filename)) =>
              //只要从kafka成功读取目录名和文件名,就删除redis上的hss:dir:xxx:files记录
              redisFileRemove(dir, filename)
              //只要从kafka成功读取目录名和文件名,插入到redis上的hss:processingfiles记录中
              redisProcessingFileInsert(dir, filename)
              ftpKafkaRouter ! DirectiveFtpKafka(dir, filename)
            case Failure(e) =>
              fileFailCount += 1
              consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getMessage}, ${e.getCause}")
          }
        case Failure(e) =>
          fileFailCount += 1
          consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getMessage}, ${e.getCause}")
      }
    }.to(Sink.ignore).run()
  }

  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        log.warning(s"Member is Removed: ${member.address} after $previousStatus")
      case DirectiveStat =>
        sender() ! DirectiveStatResult(fileCount, fileFailCount, recordCount)
        fileCount = 0
        recordCount = 0
        fileFailCount = 0
      case DirectiveFtpKafkaResult(retFileFailCount, retRecordCount) =>
        fileCount += 1
        recordCount += retRecordCount
        fileFailCount += retFileFailCount
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }
}
