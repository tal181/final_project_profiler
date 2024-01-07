package com.project.checks.utils

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.ParseException
import org.slf4j.{Logger, LoggerFactory}

import java.io.FileNotFoundException
import scala.util.Try

case class ExceptionMessageConverter() {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  def convert(exception: Throwable): String = {
    exception match {
     // case ex: MetricsException  => ex.getMessage
      case ex: ParseException    => handleSQlParserException(ex)
      case ex: AnalysisException => getAnalysisExceptionMessage(ex.message)
      case ex: SparkException    => getRootCauseMessage(ex)
      case ex: FileNotFoundException =>
        val message = getRootCauseMessage(ex)
        val index = message indexOf "FileNotFoundException"
        if (index >= 0) message.substring(index) else message
      case ex => getRootCauseMessage(ex)
    }
  }

  def handleSQlParserException(ex: ParseException): String = {
    val cause = getRootCauseMessage(ex)
    val index = cause indexOf "== SQL =="
    if (index == -1) cause
    else s"""Invalid SQL query: ${cause.drop(index + "== SQL ==".length)}"""
  }

  private def getRootCauseMessage(ex: Throwable): String = {
    Try(ExceptionUtils.getRootCauseMessage(ex)).getOrElse(ex.getMessage)
  }

  private def getAnalysisExceptionMessage(message: String): String = {
    val index = message indexOf " given input columns:"
    if (index >= 0) message.take(index).capitalize + " column" else message
  }
}
