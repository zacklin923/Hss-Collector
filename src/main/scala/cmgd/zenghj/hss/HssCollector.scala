package cmgd.zenghj.hss

import cmgd.zenghj.hss.actor._
import cmgd.zenghj.hss.common.CommonUtils._

import akka.actor.{Props, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.apache.commons.cli.{Option => CliOption, HelpFormatter, Options, DefaultParser}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

/**
  * Created by cookeem on 16/7/28.
  */
object HssCollector extends App {
  val options = new Options()
  //单参数选项
  options.addOption(CliOption.builder("s").longOpt("service")
    .desc("Service name: master or router or worker")
    .hasArg()
    .required()
    .argName("SERVICE")
    .build())
  //多参数选项
  options.addOption(CliOption.builder("p").longOpt("ports")
    .desc("Input multiple ports to start multiple service")
    .hasArgs
    .required()
    .argName("PORTS")
    .build())
  try {
    val parser = new DefaultParser()
    val cmd = parser.parse(options, args)
    val service = cmd.getOptionValue("s")
    if (service != "master" && service != "router" && service != "worker") {
      throw new Throwable()
    }
    val ports = cmd.getOptionValues("p").map(_.toInt)
    //启动处理逻辑
    service match {
      //启动CollectorMaster
      case "master" =>
        ports.foreach { port =>
          val serviceConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
            .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
            .withFallback(ConfigFactory.parseString("akka.cluster.roles = [master]"))
            .withFallback(config)
          val system = ActorSystem("hss-cluster", serviceConfig)
          val collectorMasterRef = system.actorOf(Props[CollectorMaster], "collector-master")
          import system.dispatcher
          //启动定期任务, 通知CollectorRouter进行文件夹列表任务
          system.scheduler.schedule(masterScheduleInterval.seconds, masterScheduleInterval.seconds) {
            collectorMasterRef ! DirectiveListDir
          }
          //启动定期任务, 通知GetFileWorker收集执行结果信息
          system.scheduler.schedule(masterStatInterval.seconds, masterStatInterval.seconds) {
            collectorMasterRef ! DirectiveStat
          }
        }
      //启动CollectorRouter
      case "router" =>
        ports.foreach { port =>
          val serviceConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
            .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
            .withFallback(ConfigFactory.parseString("akka.cluster.roles = [router]"))
            .withFallback(config)
          val system = ActorSystem("hss-cluster", serviceConfig)
          val collectorRouterRef = system.actorOf(Props[CollectorRouter], "collector-router")
        }
      //启动GetFileWorker
      case "worker" =>
        ports.foreach { port =>
          val serviceConfig = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=$port")
            .withFallback(ConfigFactory.parseString("akka.cluster.auto-down-unreachable-after = 5s"))
            .withFallback(ConfigFactory.parseString("akka.cluster.roles = [worker]"))
            .withFallback(config)
          val system = ActorSystem("hss-cluster", serviceConfig)
          val collectorRouterRef = system.actorOf(Props[GetFileWorker], "getfile-worker")
        }
    }
  } catch {
    case e: Throwable =>
      val formatter = new HelpFormatter()
      consoleLog("ERROR", s"${e.getMessage}, ${e.getCause}")
      formatter.printHelp("Start HSS log collector cluster.\n", options, true)
  }
}
