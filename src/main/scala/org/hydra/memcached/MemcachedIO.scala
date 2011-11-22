package org.hydra.memcached

import java.net.{ InetSocketAddress, Socket }
import java.io.{ InputStream, BufferedInputStream }
import java.util.concurrent._
import scala.collection.mutable.{ ArrayBuffer, HashMap }
import MemcachedProtocol._
import org.hydra.Logger

sealed abstract class Result
case class ErrorResult(err: String) extends Result
case object NotexistsResult extends Result
case class IntResult(value: Int) extends Result
case class SingleLineResult(msg: String) extends Result
case class BooleanResult(b: Boolean) extends Result
case class KVResult(item: KV) extends Result
case class MultiKVResult(values: HashMap[String, KV]) extends Result
case class StatsResult(stats: ArrayBuffer[Pair[String, String]]) extends Result

class ResultFuture(val cmd: Cmd) extends Future[Result] {
  private[memcached] val latch = new CountDownLatch(1)
  private[memcached] var result: Result = null

  override def get(): Result = get(10, TimeUnit.SECONDS)
  override def isCancelled(): Boolean = false
  override def cancel(p: Boolean): Boolean = false
  override def isDone(): Boolean = latch.getCount == 0

  override def get(t: Long, unit: TimeUnit): Result = {
    if (latch.await(t, unit)) result match {
      case ErrorResult(msg) => throw new MemcachedException(msg)
      case _ => result
    }
    else throw new TimeoutException
  }
}

object MemcachedConnection {
  private[memcached]type OpQueue = ArrayBlockingQueue[ResultFuture]

  private[memcached] val log = Logger.getLogger(getClass.getName)
  private[memcached] val cmdQueue = new ArrayBlockingQueue[Pair[MemcachedConnection, ResultFuture]](2048)

  val actor = scala.actors.Actor.actor {
    while (true) {
      val (conn, f) = cmdQueue.take()
      try {
        if (conn.isConnected) {
          conn.enqueue(f)
        } else {
          log.error("Skipping cmd queued up into a closed connection (%s)".format(f.cmd))
          f.result = new ErrorResult("Connection closed")
          f.latch.countDown
        }
      } catch {
        case e: Exception => {
          log.error("Exception:" + e.getMessage, e)
        }
      }
    }
  }
}

class MemcachedConnection(val host: String = "localhost", val port: Int = 11211) {
  import MemcachedConnection._

  private[MemcachedConnection] var isRunning = true
  private[MemcachedConnection] val opQueue = new OpQueue(128)
  private[MemcachedConnection] val socket = new Socket()
  socket.setTcpNoDelay(true)
  socket.setSoTimeout(1000)
  socket.setKeepAlive(true)
  socket.setReceiveBufferSize(1024 * 4)
  socket.setSendBufferSize(1024 * 4)

  socket.connect(new InetSocketAddress(host, port))

  if (log.isInfoEnabled) log.info("Connecting to %s:%s".format(host, port))

  def apply(cmd: Cmd): ResultFuture = {
    if (log.isTraceEnabled) log.trace("Exec :" + cmd)
    val f = new ResultFuture(cmd)
    cmdQueue.offer((this -> f), 10, TimeUnit.SECONDS)
    f
  }

  def enqueue(future: ResultFuture) = {
    val encd = MemcachedCommandEncoder.cmdToBinValConverter(future.cmd)
    if (log.isTraceEnabled) log.debug("Sending " + new String(encd))
    socket.getOutputStream().write(encd)
    socket.getOutputStream().flush

    future.result = MemcachedResponseDecoder.decode(future.cmd, new BufferedInputStream(socket.getInputStream()))
    future.latch.countDown
  }

  def shutdown = {
    socket.close()
    if (log.isInfoEnabled) log.info("Shutdown connection to %s ...".format((host, port)))
  }

  def isConnected(): Boolean = socket.isConnected()
}

