package cmgd.zenghj.hss.ftp

import cmgd.zenghj.hss.common.CommonUtils._

import java.io.{FileOutputStream, File, FileInputStream}

import org.apache.commons.net.ftp.{FTPFile, FTP, FTPClient}

/**
  * Created by cookeem on 16/7/25.
  */
case class FtpUtils(ftpHost: String, ftpPort: Int, ftpUser: String, ftpPass: String) {
  val ftpClient = new FTPClient()

  //连接ftp
  def connect(): Boolean = {
    try {
      //连接
      ftpClient.connect(ftpHost, ftpPort)
      ftpClient.login(ftpUser, ftpPass)
      //设置
      ftpClient.enterLocalPassiveMode()
      ftpClient.setFileType(FTP.BINARY_FILE_TYPE)
      ftpClient.setControlKeepAliveTimeout(20)
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"ftp connect error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass. ${ftpClient.getReplyString}")
        ftpClient.disconnect()
        consoleLog("ERROR", s"ftp connect error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass. ${e.getMessage} ${e.getCause}")
    }
    ftpClient.isConnected
  }

  //ftp上传文件
  def put(localPath: String, localFileName: String, remotePath: String, remoteFileName: String): Boolean = {
    var success = false
    try {
      if (!ftpClient.isConnected) {
        connect()
      }
      if (ftpClient.isConnected) {
        //上传
        val is = new FileInputStream(new File(s"$localPath/$localFileName"))
        ftpClient.storeFile(s"$remotePath$remoteFileName", is)
        is.close()
        ftpClient.disconnect()
        success = true
      } else {
        consoleLog("ERROR", s"ftp put error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass")
      }
    } catch {
      case e: Throwable =>
        ftpClient.disconnect()
        consoleLog("ERROR", s"ftp put error: localPath = $localPath, localFileName = $localFileName, remotePath = $remotePath, remoteFileName = $remoteFileName. ${e.getMessage} ${e.getCause}")
    }
    success
  }

  //ftp下载文件
  def get(localPath: String, localFileName: String, remotePath: String, remoteFileName: String): Boolean = {
    var success = false
    var errmsg = ""
    val startTime = System.currentTimeMillis()
    try {
      if (!ftpClient.isConnected) {
        connect()
      }
      if (ftpClient.isConnected) {
        //下载
        val fos = new FileOutputStream(s"$localPath/$localFileName")
        ftpClient.setBufferSize(1024)
        ftpClient.retrieveFile(s"$remotePath/$remoteFileName", fos)
        fos.close()
        ftpClient.disconnect()
        success = true
      } else {
        ftpClient.disconnect()
        errmsg = s"ftp get error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass"
      }
    } catch {
      case e: Throwable =>
        errmsg = s"ftp get error: localPath = $localPath, localFileName = $localFileName, remotePath = $remotePath, remoteFileName = $remoteFileName. ${e.getMessage} ${e.getCause}"
    }
    val duration = Math.round(System.currentTimeMillis() - startTime)
    if (errmsg != "") {
      consoleLog("ERROR", s"$errmsg # took $duration ms")
    } else {
      consoleLog("SUCCESS", s"ftp get $remotePath/$remoteFileName success # took $duration ms")
    }
    success
  }

  //ftp列表, mode(0:全部,1:目录,2:文件)
  def ls(remotePath: String, mode: Int = 0): Array[FTPFile] = {
    var ret = Array[FTPFile]()
    var errmsg = ""
    val startTime = System.currentTimeMillis()
    try {
      if (!ftpClient.isConnected) {
        connect()
      }
      if (ftpClient.isConnected) {
        ret = mode match {
          case 0 => ftpClient.listFiles(remotePath)
          case 1 => ftpClient.listFiles(remotePath).filter(f => f.isDirectory)
          case 2 => ftpClient.listFiles(remotePath).filter(f => f.isFile)
          case _ => ftpClient.listFiles(remotePath)
        }
        ftpClient.disconnect()
      } else {
        errmsg = s"ftp list error: ftpHost = $ftpHost, ftpPort = $ftpPort, ftpUser = $ftpUser, ftpPass = $ftpPass"
      }
    } catch {
      case e: Throwable =>
        ftpClient.disconnect()
        errmsg = s"ftp list error: remotePath = $remotePath, mode = $mode. ${e.getMessage} ${e.getCause}"
    }
    val duration = Math.round(System.currentTimeMillis() - startTime)
    if (errmsg != "") {
      consoleLog("ERROR", s"$errmsg # took $duration ms")
    } else {
      consoleLog("SUCCESS", s"ftp ls $remotePath success # took $duration ms")
    }
    ret
  }

  //关闭ftp
  def close(): Boolean = {
    //退出
    var success = false
    try {
      ftpClient.disconnect()
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"ftp close error: ${e.getMessage} ${e.getCause}")
    }
    success
  }
}
