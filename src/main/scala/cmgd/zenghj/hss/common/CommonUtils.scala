package cmgd.zenghj.hss.common

import java.io.File

import com.typesafe.config.ConfigFactory
import org.joda.time.DateTime

/**
  * Created by cookeem on 16/7/28.
  */
object CommonUtils {
  val config = ConfigFactory.parseFile(new File("conf/application.conf"))

  val masterConfig = config.getConfig("cluster-router.master")
  val masterScheduleInterval = masterConfig.getInt("schedule-interval")
  val masterStatInterval = masterConfig.getInt("stat-interval")

  val listFileRouterConfig = config.getConfig("cluster-router.listfile-router")
  val listFileRouterPoolSize = listFileRouterConfig.getInt("pool-size")
  val listFileRouterLowBound = listFileRouterConfig.getInt("lower-bound")
  val listFileRouterUpperBound = listFileRouterConfig.getInt("upper-bound")

  val getFileWorkerConfig = config.getConfig("cluster-router.getfile-worker")
  val getFileWorkerStreamCount = getFileWorkerConfig.getInt("stream-count")

  val ftpConfig = config.getConfig("ftp")
  val ftpHost = ftpConfig.getString("ftp-host")
  val ftpPort = ftpConfig.getInt("ftp-port")
  val ftpUser = ftpConfig.getString("ftp-user")
  val ftpPass = ftpConfig.getString("ftp-pass")
  val ftpRoot = ftpConfig.getString("ftp-root")
  val ftpLocalRoot = ftpConfig.getString("ftp-localroot")

  val redisConfig = config.getConfig("redis")
  val redisHost = redisConfig.getString("redis-host")
  val redisPort = redisConfig.getInt("redis-port")
  var redisPass = redisConfig.getString("redis-pass")

  val kafkaConfig = config.getConfig("kafka")
  val kafkaZkUri = kafkaConfig.getString("zookeeper-uri")
  val kafkaBrokers = kafkaConfig.getString("brokers-list")
  val kafkaFilesTopic = kafkaConfig.getString("kafka-files-topic")
  val kafkaRecordsTopic = kafkaConfig.getString("kafka-records-topic")
  val kafkaConsumeGroup = kafkaConfig.getString("consume-group")
  val kafkaNumPartitions = kafkaConfig.getInt("kafka-num-partitions")
  val kafkaReplication = kafkaConfig.getInt("kafka-replication")


  def consoleLog(logType: String, msg: String) = {
    val timeStr = new DateTime().toString("yyyy-MM-dd HH:mm:ss")
    println(s"[$logType] $timeStr: $msg")
  }
}
