package cmgd.zenghj.hss.actor

import cmgd.zenghj.hss.common.CommonUtils._

import akka.actor.{ActorRef, Props}
import akka.cluster.ClusterEvent.{MemberRemoved, MemberUp}
import akka.routing.{DefaultResizer, RoundRobinPool}

/**
  * Created by cookeem on 16/7/28.
  */
class CollectorRouter extends TraitClusterActor {
  val resizer = DefaultResizer(lowerBound = configListFileRouterLowBound, upperBound = configListFileRouterUpperBound)
  val listFileRouter: ActorRef = context.actorOf(RoundRobinPool(configListFileRouterPoolSize, Some(resizer)).props(Props[ListFileWorker]), "listfile-router")

  def receive: Receive =
    eventReceive.orElse {
      case MemberUp(member) =>
        log.info(s"Member is Up: ${member.address}")
      case MemberRemoved(member, previousStatus) =>
        log.warning(s"Member is Removed: ${member.address} after $previousStatus")
      case DirectiveListFile(dir) =>
        //通知ListFileWorker进行目录文件列表
        listFileRouter ! DirectiveListFile(dir)
      case e =>
        log.error(s"Unhandled message: ${e.getClass} : $e ")
    }




}
