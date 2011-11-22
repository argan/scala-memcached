package org.hydra.memcached
import junit.framework.TestCase
import junit.framework.Assert._
import org.hydra.Logger

class MemcachedClientTest extends TestCase {
  val c = MemcachedClient()
  val log = Logger.getLogger(getClass.getName)

  override def setUp = {
    c.flushall
  }

  implicit def stringToBytes(s: String): Array[Byte] = s.getBytes

  private def get(key: String): String = {
    val x = c.get(key)
    x match {
      case null => null
      case _ => new String(x.data)
    }
  }

  def testStats = {
    c.stats
    c.stats("slabs")
  }

  def testGetSet = {
    assertTrue(c.flushall)
    assertNull(get("foo"))
    assertTrue(c.set("foo", "bar"))
    assertEquals("bar", get("foo"))
    assertEquals("bar", get("foo"))
  }

  def testMisc = {
    assertTrue(c.verbose(2))
    assertTrue(c.verbose(0))
    log.debug(c.stats)
    log.debug(c.stats("slabs"))
    log.debug(c.version)
  }

  def testMget = {
    c.set("foo", "bar")
    c.set("bar", "foo")
    c.set("fofo", "barbar")
    val map = c.gets("foo", "bar")
    assertEquals("bar", new String(map.get("foo").get.data))
    assertEquals("foo", new String(map.get("bar").get.data))
    assertEquals(None, map.get("fofo"))
  }

  def testAdd = {
    assertTrue(c.add("foo", "bar"))
    assertEquals("bar", get("foo"))
    assertFalse(c.add("foo", "bar"))
    assertEquals("bar", get("foo"))
  }

  def testReplace = {
    assertEquals(null, get("foo"))
    assertFalse(c.replace("foo", "bar1"))
    assertEquals(null, get("foo"))
    assertTrue(c.add("foo", "bar"))
    assertEquals("bar", get("foo"))
    assertTrue(c.replace("foo", "bar1"))
    assertEquals("bar1", get("foo"))
  }

  def testAppend = {
    assertTrue(c.add("foo", "bar"))
    assertEquals("bar", get("foo"))
    assertTrue(c.append("foo", "xx"))
    assertEquals("barxx", get("foo"))
  }

  def testPrepend = {
    assertTrue(c.add("foo", "bar"))
    assertEquals("bar", get("foo"))
    assertTrue(c.prepend("foo", "xx"))
    assertEquals("xxbar", get("foo"))
  }

  def testCas = {
    assertTrue(c.add("foo", "bar"))
    val unique = c.gets("foo").get("foo").get.casUnique
    assertTrue(c.cas("foo", "bar1", unique))
    assertFalse(c.cas("foo", "bar2", unique))
    assertEquals("bar1", get("foo"))
  }

  def testDelete = {
    assertFalse(c.delete("foo"))
    assertTrue(c.add("foo", "bar"))
    assertEquals("bar", get("foo"))
    assertTrue(c.delete("foo"))
    assertEquals(null, c.get("foo"))
  }

  def testIncr = {
    assertNull(c.get("foo"))
    try {
      c.incr("foo")
    } catch {
      case e: Exception => assertEquals("Key not found.", e.getMessage())
    }
    assertTrue(c.set("foo", "1"))
    assertEquals(2, c.incr("foo"))
    assertEquals("2", get("foo"))
    assertEquals(4, c.incr("foo", 2))
    assertEquals("4", get("foo"))
  }
  def testDecr = {
    assertNull(c.get("foo"))
    try {
      c.incr("foo")
    } catch {
      case e: Exception => assertEquals("Key not found.", e.getMessage())
    }
    assertTrue(c.set("foo", "10"))
    assertEquals(9, c.decr("foo"))
    assertEquals("9", get("foo").trim)
    assertEquals(7, c.decr("foo", 2))
    assertEquals("7", get("foo").trim)
  }
}