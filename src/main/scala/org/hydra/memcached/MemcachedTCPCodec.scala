package org.hydra.memcached

import org.hydra.Logger

private[memcached] object MemcachedCommandEncoder {
  import MemcachedProtocol._
  import Conversions._

  val SPACE = " ".getBytes
  val EOL = "\r\n".getBytes

  def copiedArray(vals: BinVal*): BinVal = {
    val length = vals.map(v => v.length).sum
    val result = new Array[Byte](length)

    var size = 0;
    def copy(v1: BinVal, v2: BinVal): BinVal = {
      System.arraycopy(v2, 0, v1, size, v2.length); size += v2.length; v1
    }
    vals.foldLeft(result)((v1, v2) => copy(v1, v2))
    result
  }

  type StoreCommand = { def item: DataItem }
  type SingleKey = { def key: String }

  def cmdToBinValConverter(cmd: Cmd) =
    cmd match {
      case Incr(key, delta) =>
        copiedArray(cmd.name, SPACE, key, SPACE, delta, EOL)
      case Decr(key, delta) =>
        copiedArray(cmd.name, SPACE, key, SPACE, delta, EOL)
      case Cas(item: DataItem) =>
        copiedArray(cmd.name, SPACE, item.key, SPACE, item.flags, SPACE, item.exptime, SPACE, item.data.length, SPACE, item.casUnique, EOL, item.data, EOL)
      case v0 if (v0.isInstanceOf[Add] || v0.isInstanceOf[Set] || v0.isInstanceOf[Replace] || v0.isInstanceOf[Prepend] || v0.isInstanceOf[Append]) =>
        val v = v0.asInstanceOf[StoreCommand]
        copiedArray(cmd.name, SPACE, v.item.key, SPACE, v.item.flags, SPACE, v.item.exptime, SPACE, v.item.data.length, EOL, v.item.data, EOL)
      case v if (v.isInstanceOf[Get] || v.isInstanceOf[Delete] || v.isInstanceOf[Touch]) =>
        val v0 = v.asInstanceOf[SingleKey]
        copiedArray(cmd.name, SPACE, v0.key, EOL)
      case v: Gets =>
        copiedArray(cmd.name, SPACE, v.keys.toList.mkString(" "), EOL)
      case Stats(args) =>
        copiedArray(cmd.name, SPACE, args, EOL)
      case Verbosity(verbose) =>
        copiedArray(cmd.name, SPACE, verbose, EOL)
      case _ =>
        copiedArray(cmd.name, EOL)
    }

}

private[memcached] object MemcachedResponseDecoder {
  import java.io.{ BufferedInputStream, InputStream }
  import scala.collection.mutable.{ ArrayBuffer, HashMap }
  import MemcachedProtocol._

  private val log = Logger.getLogger(getClass.getName)

  def decode(cmd: Cmd, buff: BufferedInputStream): Result = {
    val line = readLine(buff)
    val arr: Array[String] = line.split(" ")
    if (log.isTraceEnabled) log.trace("Line:" + line)
    arr match {
      // common results
      case Array("ERROR") => ErrorResult("")
      case Array("CLIENT_ERROR", _*) => ErrorResult(line)
      case Array("SERVER_ERROR", _*) => ErrorResult(line)
      // for store commands : touch/delete/set/replace/add/prepend/append/cas
      case Array("STORED") => BooleanResult(true)
      case Array("DELETED") => BooleanResult(true)
      case Array("TOUCHED") => BooleanResult(true)
      case Array("OK") => BooleanResult(true)

      case Array("NOT_STORED") => BooleanResult(false)
      case Array("EXISTS") => BooleanResult(false)
      case Array("NOT_FOUND") => BooleanResult(false)

      case Array("VERSION", _*) => SingleLineResult(line)

      case _ =>
        decodeData(cmd, arr, buff)
    }
  }

  def decodeData(cmd: Cmd, arr: Array[String], buff: BufferedInputStream): Result = cmd match {
    case c if (c.isInstanceOf[Incr] || c.isInstanceOf[Decr]) =>
      IntResult(Integer.valueOf(arr(0)))
    case c: Get =>
      arr match {
        // for retrieve command : get/gets
        // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
        //   <data block>\r\n
        case Array("VALUE", key, flags, bytes) =>
          val len = Integer.valueOf(bytes)
          val data = read(buff, len)

          KVResult(KV(key, data, Integer.valueOf(flags)))
        case Array("END") =>
          NotexistsResult
      }
    case c: Gets => {
      decodeMultiValue(arr, MultiKVResult(new HashMap[String, KV]()), buff)
    }
    case c: Stats => {
      decodeStats(arr, StatsResult(new ArrayBuffer[Pair[String, String]]), buff)
    }
  }

  def decodeMultiValue(arr: Array[String], result: MultiKVResult, buff: BufferedInputStream): Result = arr match {
    // for retrieve command : get/gets
    // VALUE <key> <flags> <bytes> [<cas unique>]\r\n
    //   <data block>\r\n
    case Array("VALUE", key, flags, bytes, casUnique) =>
      val len = Integer.valueOf(bytes)
      val data = read(buff, len)
      result.values.put(key, KV(key, data, Integer.valueOf(flags), java.lang.Long.valueOf(casUnique)))
      val line = readLine(buff)
      val narr = line.split(" ")
      decodeMultiValue(narr, result, buff)
    case Array("END") =>
      result
    case _ =>
      if (log.isErrorEnabled) log.error("Line [" + arr.mkString(" ") + "] size:[" + arr.length + "] doesn't match any pattern.")
      ErrorResult("Unknown error,line:" + arr.mkString(" "))
  }

  def decodeStats(arr: Array[String], result: StatsResult, buff: BufferedInputStream): Result = arr match {
    case Array("STAT", key, value) =>
      result.stats.append(Pair(key, value))
      val line = readLine(buff)
      val narr = line.split(" ")
      decodeStats(narr, result, buff)
    case Array("END") =>
      result
  }

  /**
   * read a line,until \r\n or EOF
   */
  def readLine(in: InputStream): String = {
    val buff = new ArrayBuffer[Byte]

    val byteArr = new Array[Byte](1)
    def nextByte(in: InputStream): Byte = {
      var count = in.read(byteArr)
      if (count != -1) {
        byteArr(0)
      } else {
        -1
      }
    }

    var curr = nextByte(in)
    var next = nextByte(in)

    var found = false

    while (!found && curr != -1) {
      if (curr == '\r' && next == '\n') {
        found = true
      } else {
        buff.append(curr)
        curr = next
        next = nextByte(in)
      }
    }

    new String(buff.toArray)
  }

  /**
   * read given size from InputStream
   */
  def read(in: InputStream, size: Int): BinVal = {
    val result = new BinVal(size)
    var count = in.read(result, 0, size)
    var total = count;
    if (count < size) {
      count = in.read(result, total, size - total)
      total += count
      while (count != -1 && total < size) {
        count = in.read(result, total, size - total)
        if (count != -1) {
          total += count
        }
      }
    }
    // read the '\r\n' after data block 
    in.read(new BinVal(2), 0, 2)
    result
  }

  /**
   * read all data available from InputStream
   */
  def read(in: InputStream): BinVal = {
    import scala.collection.mutable.ArrayBuffer
    val buffers = new ArrayBuffer[Pair[BinVal, Int]]()
    var buff = new BinVal(1024)

    var size = in.read(buff)
    while (size != -1) {
      buffers.append(Pair(buff, size))
      buff = new BinVal(1024)
      size = in.read(buff)
    }

    val totalSize = buffers.map(_._2).sum
    val result = new BinVal(totalSize)

    var count = 0
    def copy(v1: BinVal, v2: BinVal, length: Int): BinVal = {
      System.arraycopy(v2, 0, v1, size, length); size += v2.length; v1
    }
    buffers.foldLeft(result)((v1, x) => copy(v1, x._1, x._2))
    result
  }
}

trait StringConversions {
  implicit def convertStringToByteArray(s: String): Array[Byte] = s.getBytes
  implicit def convertByteArrayToString(b: Array[Byte]): String = new String(b)
}

trait IntToStringConversions {
  implicit def convertIntToStringBytes(i: Int): Array[Byte] = String.valueOf(i).getBytes
  implicit def convertLongToStringBytes(i: Long): Array[Byte] = String.valueOf(i).getBytes
}

class Conversions
object Conversions extends Conversions with StringConversions with IntToStringConversions
