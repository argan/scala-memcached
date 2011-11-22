package org.hydra
import java.util.WeakHashMap

object Logger {
  val cache = new WeakHashMap[String, Logger]()

  val level = level0

  private def level0 = {
    var str = System.getenv().get("LOG")

    if (str == null) str = "INFO"

    str = str.toUpperCase

    str match {
      case "TRACE" => 0
      case "DEBUG" => 1
      case "INFO" => 2
      case "WARN" => 3
      case "ERROR" => 4
      case "FATAL" => 5
      case _ => 2 // default info
    }
  }

  def getLogger(name: String) = {
    var opt = cache.get(name)
    if (opt == null) {
      opt = new Logger(name)
      cache.put(name, opt)
    }
    opt
  }
}

class Logger(val name: String) {
  import Logger.level

  def isTraceEnabled = level < 1
  def isDebugEnabled = level < 2
  def isInfoEnabled = level < 3
  def isWarnEnabled = level < 4
  def isErrorEnabled = level < 5
  def isFatalEnabled = level < 6

  def trace(msg: Any) = if (isTraceEnabled) p("TRACE", msg)
  def debug(msg: Any) = if (isDebugEnabled) p("DEBUG", msg)
  def info(msg: Any) = if (isInfoEnabled) p("INFO ", msg)
  def warn(msg: Any) = if (isWarnEnabled) p("WARN ", msg)
  def error(msg: Any) = if (isErrorEnabled) p("ERROR", msg)
  def fatal(msg: Any) = if (isFatalEnabled) p("FATAL", msg)

  private def p(lvl: String, msg: Any) {
    println(lvl + " " + name + " " + msg)
  }
}