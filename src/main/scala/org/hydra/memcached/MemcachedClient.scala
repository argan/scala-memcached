package org.hydra.memcached

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import scala.collection.mutable.{ Buffer, HashMap }

object MemcachedClient {
  def apply(host: String = "localhost", port: Int = 11211, initSize: Int = 1, maxSize: Int = 10) = new MemcachedClient(new ConnectionQueue(host, port, initSize, maxSize))
}

class MemcachedClient(val queue: ConnectionQueue) {
  import MemcachedProtocol._

  val THIRTY_DAYS = 30 * 24 * 60 * 60

  /**
   * ensure the connection be offered back to the queue after used
   */
  private def withConn[R](callback: MemcachedConnection => R): R = {
    val conn = queue.take
    try {
      callback(conn)
    } finally {
      if (conn != null) {
        queue.offer(conn)
      }
    }
  }

  def isConnected = withConn[Boolean](conn => conn.isConnected)

  def exec(cmd: Cmd) = withConn[ResultFuture](conn => conn(cmd))

  // store commands
  private def createItem(key: String, data: Array[Byte], flags: Int = 0, exptime: Int = THIRTY_DAYS, casUnique: Long = -1L): DataItem = {
    DataItem(key, flags, exptime, data.length, casUnique, data)
  }

  private def store(cmd: Cmd): Boolean = exec(cmd).get.asInstanceOf[BooleanResult].b
  def set(key: String, value: Array[Byte]): Boolean = store(Set(createItem(key, value)))
  def add(key: String, value: Array[Byte]): Boolean = store(Add(createItem(key, value)))
  def replace(key: String, value: Array[Byte]): Boolean = store(Replace(createItem(key, value)))
  def prepend(key: String, value: Array[Byte]): Boolean = store(Prepend(createItem(key, value)))
  def append(key: String, value: Array[Byte]): Boolean = store(Append(createItem(key, value)))
  def cas(key: String, value: Array[Byte], casUnique: Long): Boolean = store(Cas(createItem(key, value, casUnique = casUnique)))

  private def incrdecr(cmd: Cmd): Int = {
    val res = exec(cmd).get
    res match {
      case IntResult(i) => i
      case BooleanResult(false) => throw MemcachedException("Key not found.")
    }
  }

  def incr(key: String, delta: Int = 1): Int = incrdecr(Incr(key, delta))
  def decr(key: String, delta: Int = 1): Int = incrdecr(Decr(key, delta))

  def delete(key: String): Boolean = exec(Delete(key)).get.asInstanceOf[BooleanResult].b

  // retrieve commands
  def get(key: String): KV = {
    val result = exec(Get(key)).get
    result match {
      case NotexistsResult => null
      case KVResult(item) => item
    }
  }

  def gets(keys: String*): HashMap[String, KV] = {
    exec(Gets(keys.toList)).get.asInstanceOf[MultiKVResult].values
  }

  // other commands
  def verbose(level: Int): Boolean = exec(Verbosity(level)).get.asInstanceOf[BooleanResult].b
  def version: String = exec(Version()).get.asInstanceOf[SingleLineResult].msg

  def stats(args: String): Buffer[Pair[String, String]] = exec(Stats(args)).get.asInstanceOf[StatsResult].stats
  def stats(): Buffer[Pair[String, String]] = exec(Stats("")).get.asInstanceOf[StatsResult].stats

  def flushall: Boolean = exec(FlushAll()).get.asInstanceOf[BooleanResult].b

}
