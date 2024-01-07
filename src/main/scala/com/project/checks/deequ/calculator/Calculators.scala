package com.project.checks.deequ.calculator

import com.project.checks.domain.{CheckEvaluation, CheckResult, Metric, MetricResult, SqlCheckConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

object Calculators {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def onErrorMarkAllMetricsAsFailed(metrics: Seq[Metric])(ex: Throwable): Seq[MetricResult] = {
    logger.debug(s"Failed reading the table. Failing all the metrics.\n ${ex.getMessage}")
    val results = metrics.map {
      case metric => MetricResult(metric, Failure(ex))
    }
    results
  }

  def onErrorMarkAllChecksAsFailed(checks: Seq[SqlCheckConfig])(ex: Throwable): Seq[CheckEvaluation] = {
    logger.debug(s"Failed reading the table. Failing all the metrics.\n ${ex.getMessage}")
    val results = checks.map {
      case check => CheckEvaluation(check.checkName, Failure(ex))
    }
    results
  }


}
