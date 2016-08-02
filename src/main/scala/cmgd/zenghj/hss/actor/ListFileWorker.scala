package cmgd.zenghj.hss.actor

import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.kafka.KafkaUtils._
import cmgd.zenghj.hss.redis.RedisUtils._

import akka.actor._

/**
  * Created by cookeem on 16/7/28.
  */
class ListFileWorker extends Actor with ActorLogging {
  def receive = {
    case DirectiveStopMember =>
      self ! PoisonPill
    case DirectiveListFile(dir) =>
      //进行目录文件列表工作,并且写入redis
      val newFiles: Array[String] = listFiles(dir)
      //把新的文件名写入到kafka中
      val newDirFiles = newFiles.map(filename => (dir, filename))
      filenameSinkKafka(newDirFiles)
    case e =>
      log.error(s"Unhandled message: ${e.getClass} : $e ")
  }

  val ftpUtils = new FtpUtils(ftpHost, ftpPort, ftpUser, ftpPass)

  //进行文件列表工作,并且返回新的文件列表, Array[filename], 不包含dir
  def listFiles(dir: String): Array[String] = {
    val remotePath = s"$ftpRoot/$dir"
    val filesArr: Array[String] = ftpUtils.ls(remotePath, 2).map(_.getName).sorted
    val newFileArr = redisFilesInsert(dir, filesArr)
    newFileArr
  }
}
