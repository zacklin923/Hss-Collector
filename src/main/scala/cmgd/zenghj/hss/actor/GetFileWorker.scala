package cmgd.zenghj.hss.actor

import java.io.File

import cmgd.zenghj.hss.common.CommonUtils._

import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.NotUsed
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils._
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

  var fileCount = 0
  var recordCount = 0
  var fileFailCount = 0

  //启动从kafka读取文件列表, 下载ftp文件后,把文件记录入库到kafka的reactive kafka stream
  (1 to getFileWorkerStreamCount).foreach { i =>
    ftpToKafkaStream()
  }

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
          val tryObj2 = Try(obj.asInstanceOf[Tuple2[String, String]])
          tryObj2 match {
            case Success((dir, filename)) =>
              //进行ftp文件下载,并把文件写入到kafka
              val ftpUtils = FtpUtils(ftpHost, ftpPort, ftpUser, ftpPass)
              val file = new File(s"$ftpLocalRoot/$dir")
              if (!file.exists()) {
                file.mkdirs()
              }
              val localPath = file.getAbsolutePath
              val remotePath = s"$ftpRoot/$dir"
              val ftpGetSuccess = ftpUtils.get(localPath, filename, remotePath, filename)
              var returnCount = 0
              if (ftpGetSuccess) {
                val absFilename = s"$localPath/$filename"
                returnCount = fileSinkKafka(absFilename)
                fileCount += 1
                recordCount += returnCount
              } else {
                fileFailCount += 1
              }
              //完成kafka入库后删除redis上的记录
              redisFileRemove(dir, filename)
            case Failure(e) =>
              consoleLog("ERROR", s"kafka consume files topic serialize error: ${e.getMessage}, ${e.getCause}")
          }
        case Failure(e) =>
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
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }
}
