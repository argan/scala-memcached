package org.hydra.memcached

object MemcachedProtocol {
  type BinVal = Array[Byte]
  // key,flags,exptime,length,data
  // as return value
  case class KV(key: String, data: BinVal, flags: Int, casUnique: Long = -1)
  // as input value
  case class DataItem(key: String, flags: Int, exptime: Int, length: Int, casUnique: Long, data: BinVal)

  sealed abstract class Cmd {
    def name: BinVal
  }
  /**
   * Storage commands (there are six: "set", "add", "replace", "append"
   * "prepend" and "cas") ask the server to store some data identified by a
   * key. The client sends a command line, and then a data block; after
   * that the client expects one line of response, which will indicate
   * success or failure.
   *
   * <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
   * cas <key> <flags> <exptime> <bytes> <cas unique> [noreply]\r\n
   *
   * <command name> is "set", "add", "replace", "append" or "prepend"
   */

  case class Set(item: DataItem) extends Cmd {
    def name = "set".getBytes
  }
  case class Add(item: DataItem) extends Cmd {
    def name = "add".getBytes
  }
  case class Replace(item: DataItem) extends Cmd {
    def name = "replace".getBytes
  }
  case class Append(item: DataItem) extends Cmd {
    def name = "append".getBytes
  }
  case class Prepend(item: DataItem) extends Cmd {
    def name = "prepend".getBytes
  }
  case class Cas(item: DataItem) extends Cmd {
    def name = "cas".getBytes
  }
  /**
   * Retrieval commands (there are two: "get" and "gets") ask the server to
   * retrieve data corresponding to a set of keys (one or more keys in one
   * request). The client sends a command line, which includes all the
   * requested keys; after that for each item the server finds it sends to
   * the client one response line with information about the item, and one
   * data block with the item's data; this continues until the server
   * finished with the "END" response line.
   */
  case class Get(key: String) extends Cmd {
    def name = "get".getBytes
  }
  case class Gets(keys: List[String]) extends Cmd {
    def name = "gets".getBytes
  }

  /**
   * Other commands about data
   */
  case class Delete(key: String) extends Cmd {
    def name = "delete".getBytes
  }
  case class Incr(key: String, delta: Int = 1) extends Cmd {
    def name = "incr".getBytes
  }
  case class Decr(key: String, delta: Int = 1) extends Cmd {
    def name = "decr".getBytes
  }
  case class Touch(key: String, exptime: Int) extends Cmd {
    def name = "touch".getBytes
  }
  // Misc commands
  case class Stats(args: String) extends Cmd {
    def name = "stats".getBytes
  }
  case class Version() extends Cmd {
    def name = "version".getBytes
  }
  case class FlushAll() extends Cmd {
    def name = "flush_all".getBytes
  }
  case class Quit() extends Cmd {
    def name = "quit".getBytes
  }
  case class Verbosity(verbose: Int) extends Cmd {
    def name = "verbosity".getBytes
  }

  case class MemcachedException(msg: String) extends RuntimeException(msg)
}
