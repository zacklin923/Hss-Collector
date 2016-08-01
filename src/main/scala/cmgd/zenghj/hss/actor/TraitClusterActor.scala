package cmgd.zenghj.hss.actor

import akka.actor.{Address, ActorLogging, Actor}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

import scala.collection.JavaConversions._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by cookeem on 16/7/25.
  */
trait TraitClusterActor extends Actor with ActorLogging {
  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    context.system.terminate()
  }

  def eventReceive: Receive = {
    case LeaderChanged(addr) =>
      log.info(s"Leader changed: $addr")
    case MemberJoined(member) =>
      log.info(s"Member joined: ${member.address}")
    case UnreachableMember(member) =>
      log.warning(s"Member Unreachable: ${member.address}")
      cluster.leave(member.address)
    case MemberExited(member) =>
      log.warning(s"Member is Exited: ${member.address}")
    case DirectiveStopMember =>
      cluster.down(cluster.selfAddress)
      log.error(s"Member is stop! ${cluster.selfAddress}")
  }

  //定义集群member removed关闭事件
  cluster.registerOnMemberRemoved{
    //更新seed nodes
    cluster.system.registerOnTermination(System.exit(0))
    cluster.system.terminate()
    new Thread {
      override def run(): Unit = {
        if (Try(Await.ready(cluster.system.whenTerminated, 10.seconds)).isFailure)
          System.exit(-1)
      }
    }.start()
  }
}
