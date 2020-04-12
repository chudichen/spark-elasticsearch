package com.michael.utils

import org.apache.log4j.{Level, Logger}

object LoggerLevels {

  def setStreamingLogLevels() {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}