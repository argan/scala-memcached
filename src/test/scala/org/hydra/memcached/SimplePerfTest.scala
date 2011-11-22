package org.hydra.memcached
import junit.framework.TestCase

class SimplePerfTest extends TestCase {
  val c = MemcachedClient()

  override def setUp = c.flushall

  def test = {
    testGet(1000, true)
    testGet(100000, true)
  }

  def testGet(count: Int, p: Boolean) = {
    var i = 0
    val t = System.nanoTime()
    c.set("foox", "barx".getBytes)
    while (i < count) {
      c.get("foox")
      i += 1
    }
    if (p) {
      val ts = System.nanoTime() - t
      printf("Run get %d times for %d ns, avg %d ns,tps %f \n", count, ts, ts / count, count * 1000000000.0 / ts)
    }
  }
}

