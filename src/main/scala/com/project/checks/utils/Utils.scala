package com.project.checks.utils

import com.project.checks.domain.{Metric, MetricResult}
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Failure

object Utils {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def time[T](strategy: String, block: => T): T = {
    logger.debug(s"Starting measuring time for $strategy")
    val before = System.currentTimeMillis

    val result = block

    val after = System.currentTimeMillis
    logger.info(s"====================================Elapsed time for $strategy: " + (after - before) / 1000 + " seconds============================================")
    result

  }

  def getAttribute(item: Config, attributeName: String): String = {
    try {
      item.getString(attributeName)
    }
    catch {
      case e: Exception => {
        ""
      }
    }
  }

  def prettify[T](seq: Iterable[T]): String = seq.mkString("\n\t")

  object extractors {


    object DefinitionNameEq {
      def unapply(metric: Metric): Option[(String, String)] = {
        Some(metric.definitionName, metric.column)

      }
    }
  }
}
