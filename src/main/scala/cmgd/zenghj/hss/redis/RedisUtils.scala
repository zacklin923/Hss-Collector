package cmgd.zenghj.hss.redis

import cmgd.zenghj.hss.common.CommonUtils._

import redis.clients.jedis._

import scala.collection.JavaConversions._

/**
  * Created by cookeem on 16/7/27.
  */
object RedisUtils {
  val poolConfig = new JedisPoolConfig()
  var pool: JedisPool = null
  if (redisPass == "") {
    pool = new JedisPool(poolConfig, redisHost, redisPort, 5000)
  } else {
    pool = new JedisPool(poolConfig, redisHost, redisPort, 5000, redisPass)
  }

  var jedis: Jedis = null
  try {
    jedis = pool.getResource
  } catch {
    case e: Throwable =>
  } finally {
    if (jedis != null) {
      jedis.close()
    }
  }

  //更新hss:dirs表
  def redisDirsUpdate(dirs: Array[String]) = {
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val transaction = jedis.multi()
      dirs.foreach { dir =>
        transaction.sadd("hss:dir", dir)
      }
      transaction.exec()
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisDirsUpdate error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
  }

  //更新文件列表,返回新增文件清单Array[filename],不包含dir
  def redisFilesInsert(dir: String, filesArr: Array[String]): Array[String] = {
    var newFileArr = Array[String]()
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      if (filesArr.nonEmpty) {
        var currentLastFile = jedis.get(s"hss:dir:$dir:lastfile")
        if (currentLastFile == null) {
          currentLastFile = ""
        }
        val lastfile = filesArr.last
        if (lastfile > currentLastFile) {
          val transaction = jedis.multi()
          transaction.set(s"hss:dir:$dir:lastfile", lastfile)
          filesArr.foreach { filename =>
            if (filename > currentLastFile) {
              transaction.sadd(s"hss:dir:$dir:files", filename)
              newFileArr = newFileArr :+ filename
            }
          }
          transaction.exec()
        }
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFilesInsert error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    newFileArr
  }

  def redisFileAvail(dir: String, filename: String) = {
    var available = false
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      val found = jedis.sismember(s"hss:dir:$dir:files", filename)
      if (found) {
        available = true
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFileAvail error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    available
  }

  def redisFileRemove(dir: String, filename: String) = {
    var success = false
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      jedis.srem(s"hss:dir:$dir:files", filename)
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFileRemove error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    success
  }

  def redisFilesGet(dir: String): Array[String] = {
    var jedis: Jedis = null
    var files = Array[String]()
    try {
      jedis = pool.getResource
      jedis.smembers(s"hss:dir:$dir:files").foreach { file =>
        files = files :+ file
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisFilesGet error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    files
  }

  def redisProcessingFileInsert(dir: String, filename: String) = {
    var success = false
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      jedis.sadd(s"hss:processingfiles", s"$dir:$filename")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileInsert error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    success
  }

  def redisProcessingFilesGet() = {
    var jedis: Jedis = null
    var files = Array[(String, String)]()
    try {
      jedis = pool.getResource
      jedis.smembers(s"hss:processingfiles").foreach { file =>
        val fileSplit = file.split(":")
        if (fileSplit.length == 2) {
          val dir = fileSplit(0)
          val filename = fileSplit(1)
          files = files :+ (dir, filename)
        }
      }
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFilesGet error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    files
  }

  def redisProcessingFileRemove(dir: String, filename: String) = {
    var success = false
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      jedis.srem(s"hss:processingfiles", s"$dir:$filename")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileRemove error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    success
  }

  def redisProcessingFileDeleteAll() = {
    var success = false
    var jedis: Jedis = null
    try {
      jedis = pool.getResource
      jedis.del(s"hss:processingfiles")
      success = true
    } catch {
      case e: Throwable =>
        consoleLog("ERROR", s"redisProcessingFileDeleteAll error: ${e.getMessage}, ${e.getCause}")
    } finally {
      if (jedis != null) {
        jedis.close()
      }
    }
    success
  }

}
