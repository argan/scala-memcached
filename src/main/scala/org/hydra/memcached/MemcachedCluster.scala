package org.hydra.memcached

import java.util.concurrent.atomic.AtomicReference

case class MemcachedHost(host: String, port: Int)

class MemcachedCluster[Shard](hash: (Shard) => Int, hosts: MemcachedHost*) {
  val h2cRef = new AtomicReference[Map[MemcachedHost, MemcachedClient]](Map.empty)

  def apply(s: Shard): MemcachedClient = {
    val h = hosts(hash(s).abs % hosts.length)
    h2cRef.get.get(h) match {
      case Some(c) => if (c.isConnected) c else newClient(h)
      case None => newClient(h)
    }
  }

  private[MemcachedCluster] def newClient(h: MemcachedHost): MemcachedClient = h.synchronized {
    var h2c = h2cRef.get
    def newClientThreadSafe(): MemcachedClient = {
      val c = MemcachedClient(h.host, h.port)
      while (!h2cRef.compareAndSet(h2c, h2c + (h -> c))) h2c = h2cRef.get
      c
    }
    h2c.get(h) match {
      case None => newClientThreadSafe()
      case Some(c) => if (c.isConnected) { c } else { newClientThreadSafe() }
    }
  }
}

