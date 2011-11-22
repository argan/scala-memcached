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
  // init connections
  (1 to initSize).foreach(i => queue.offer(create))

  def create = queue.synchronized {
    if (created.get() < maxSize) {
      val conn = new MemcachedConnection(host, port)
      created.addAndGet(1)
      if (log.isInfoEnabled) log.info("create new connection to " + (host, port) + " ,total " + created.get)
      conn
    } else {
      null
    }
  }

  def take: MemcachedConnection = {
    var conn = queue.poll
    // can't poll from queue,create a new
    if (conn == null) conn = create
    // if can't create new (maxSize reached), wait from queue
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
