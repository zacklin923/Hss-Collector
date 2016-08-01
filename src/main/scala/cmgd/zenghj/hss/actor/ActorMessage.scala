package cmgd.zenghj.hss.actor

/**
  * Created by cookeem on 16/7/27.
  */
case object DirectiveStopMember

//列表获取所有目录指令
case object DirectiveListDir
//让ListFileWorker列出目录下所有文件的指令
case class DirectiveListFile(dir: String)
