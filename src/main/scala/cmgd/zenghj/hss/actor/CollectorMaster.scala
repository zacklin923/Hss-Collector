package cmgd.zenghj.hss.actor

import akka.actor.RootActorPath
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import cmgd.zenghj.hss.common.CommonUtils._
import cmgd.zenghj.hss.ftp.FtpUtils
import cmgd.zenghj.hss.redis.RedisUtils._

import akka.routing.{Router, RoundRobinRoutingLogic}

/**
  * Created by cookeem on 16/7/28.
  */
class CollectorMaster extends TraitClusterActor {
  val routees = Vector[akka.routing.ActorRefRoutee]()
  var router = Router(RoundRobinRoutingLogic(), routees)

  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        if (member.hasRole("router")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "collector-router")
          router = router.addRoutee(ref)
          consoleLog("INFO", "current router:\n"+router.routees.mkString("\n"))
        }
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        if (member.hasRole("router")) {
          val ref = context.actorSelection(RootActorPath(member.address) / "user" / "collector-router")
          router = router.removeRoutee(ref)
          consoleLog("INFO", "current router:\n"+router.routees.mkString("\n"))
        }
        log.warning(s"Member is Removed: ${member.address} after $previousStatus")
      //通过ftp获取dir列表, 通知所有CollectorRouter
      case DirectiveListDir =>
        val currentDirs = listDir()
        currentDirs.foreach { dir =>
          router.route(DirectiveListFile(dir), self)
        }
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }

  val ftpUtils = new FtpUtils(ftpHost, ftpPort, ftpUser, ftpPass)

  //从ftp上获取最新的dir列表
  //返回: Array[dir]
  def listDir(): Array[String] = {
    val dirLv1Arr = ftpUtils.ls(ftpRoot, 1).map(_.getName)
    val dirs: Array[String] = dirLv1Arr.map{ dirLv1 =>
      val dirLv2Arr = ftpUtils.ls(s"$ftpRoot/$dirLv1", 1).map(_.getName)
      (dirLv1 , dirLv2Arr)
    }.flatMap{ case (dirLv1 , dirLv2Arr) => dirLv2Arr.map{dirLv2 => s"$dirLv1/$dirLv2"}}
    redisDirsUpdate(dirs)
    dirs
  }
}
