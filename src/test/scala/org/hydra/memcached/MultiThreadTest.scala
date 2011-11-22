package org.hydra.memcached
import java.util.concurrent.CountDownLatch
import org.hydra.Logger
import java.util.concurrent.TimeUnit
import junit.framework.TestCase
import junit.framework.Assert._

class MultiThreadTest extends TestCase {

  val c = MemcachedClient("localhost", 11211, 1, 4)

  override def setUp = c.flushall

  def testGetSet = {
    run(10, 10000)
    println("finished warmup")
    run(10, 10000)
    run(10, 10000)
  }

  def run(count: Int, total: Int) = {
    var latch = new CountDownLatch(count)
    var threads = (1 to count).map(i => new Thread(new Runner(i, total, c, latch)))

    val t = System.nanoTime()
    threads.foreach(_.start)

    latch.await(10, TimeUnit.SECONDS)
    if (latch.getCount() > 0) latch.await(30, TimeUnit.SECONDS)
    if (latch.getCount() == 0) {
      val ts = System.nanoTime() - t
      val tps = (count * total) / (ts / 1000 / 1000 / 1000.0)
      printf("%d threads each run %d times,total time:%d ns , avg:%d ns,tps:%f\n", count, total, ts, ts / (count * total), tps)
    }
    println("Finished")
  }

}

class Runner(i: Int, total: Int, c: MemcachedClient, latch: CountDownLatch) extends Runnable {
  implicit def stringToBytes(s: String): Array[Byte] = s.getBytes
  implicit def bytesToString(b: String): String = new String(b)

  def run = {
    (1 to total).map(j => {
      c.set(i + "foo" + j, i + "bar" + j)
      assertEquals(i + "bar" + j, new String(c.get(i + "foo" + j).data))
    })
    latch.countDown()
  }
}
