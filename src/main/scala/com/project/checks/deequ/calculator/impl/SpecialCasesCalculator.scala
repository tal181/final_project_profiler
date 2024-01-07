package com.project.checks.deequ.calculator.impl

import com.project.checks.deequ.calculator.MetricsCalculator.convertToString
import SpecialCasesCalculator.CountColumns
import com.project.checks.deequ.calculator.MetricsCalculator
import com.project.checks.domain.{Metric, MetricResult, Table}
import com.project.checks.exceptions.MetricValueIsNullOrIllegal
import com.project.checks.utils.Utils.extractors.DefinitionNameEq
import com.project.checks.utils.Utils.prettify
import org.apache.spark.sql.catalyst.util.DateTimeConstants.MICROS_PER_SECOND
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import scala.util.{Failure, Success}

/**
  * count columns and run timestamp metrics are special in a way they don't require data scanning
  * hence we have a dedicated calculator for them
  */
case class SpecialCasesCalculator(override protected val next: Option[MetricsCalculator] = None)
  extends MetricsCalculator {

  def calculate(table: Table, metrics: Seq[Metric]): Seq[MetricResult] = {
    logger.debug(s"[Start] Calculating the  special metrics")

    val calculateResult: PartialFunction[Metric, MetricResult] = {
      case metric @ DefinitionNameEq(CountColumns, _) =>
        toMetricResult(metric, schemaSize(table))
    }

    val specialCasesMetrics = metrics.collect {
      //it should be by the name since ids are different in each environment
      case metric @ DefinitionNameEq(CountColumns, _) => metric
    }

    log(specialCasesMetrics)

    val results = specialCasesMetrics.map(calculateResult)
    val nextResults = runNext(table, (metrics.toSet -- specialCasesMetrics.toSet).toSeq)
      .map(recoverWithRefWhenMetricResultIsNull)

    logger.debug(s"[End] Calculating the metrics")
    results ++ nextResults
  }

  private def schemaSize(table: Table): Int = {
    if (table.datum.schema.fields.distinct.size == table.datum.schema.size) {
      return 0
    }
    1
  }

  private def toMetricResult(metric: Metric, value: Double): MetricResult =
    MetricResult(metric, Success(value).map(convertToString))

  private def log(specialCasesMetrics: Seq[Metric]): Unit = {
    logger.debug(s"The number of special cases metrics is ${specialCasesMetrics.size}")

    if (specialCasesMetrics.nonEmpty) {
      logger.debug(s"The list of special cases metrics metrics is \n\t${prettify(specialCasesMetrics)}")
    }
  }


  private def recoverWithRefWhenMetricResultIsNull: PartialFunction[MetricResult, MetricResult] = {
    //null requires a special treatment
    //it might be returned by some built-in function in Spark, e.g. unix_timestamp
    case MetricResult(metric, Success(value)) if value == null =>
      logger.debug(s"Recovering with #REF! for the metric that returned null: $metric")
      MetricResult(metric, Failure(MetricValueIsNullOrIllegal()))
    case x => x
  }
}

object SpecialCasesCalculator {
  val CountColumns = "duplicate columns"
}
