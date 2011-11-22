package org.hydra.memcached
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.LinkedBlockingQueue
import org.hydra.Logger

class ConnectionQueue(host: String, port: Int, var initSize: Int, maxSize: Int) {
  val log = Logger.getLogger(getClass.getName)
  val queue = new LinkedBlockingQueue[MemcachedConnection]()

  val created = new AtomicInteger()

  initSize = List(initSize, maxSize).min

  (1 to initSize).foreach(i => queue.offer(create))

  def create = {
    val conn = new MemcachedConnection(host, port)
    created.addAndGet(1)
    if (log.isInfoEnabled) log.info("create new connection to " + (host, port) + " ,total " + created.get)
    conn
  }

  def take: MemcachedConnection = queue.synchronized {
    var conn = queue.poll
    while (conn == null && created.get() < maxSize) {
      queue.offer(create)
      conn = queue.poll
    }
    if (conn == null) {
      if (log.isInfoEnabled) log.info("waiting for available connection to " + (host, port))
      queue.take
    } else {
      conn
    }
  }
  def offer(conn: MemcachedConnection) = {
    queue.offer(conn)
  }
}
