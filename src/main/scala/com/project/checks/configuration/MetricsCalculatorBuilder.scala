package com.project.checks.configuration

import com.project.checks.deequ.calculator.impl.{MetricsNotifierProxy, SpecialCasesCalculator}
import com.project.checks.deequ.calculator.{DeequMetricsCalculator, MetricsCalculator, MetricsQueryParser, impl}
import com.project.checks.sql.{QueryPerMetricCalculator, SqlRunner}
import org.apache.spark.sql.SparkSession

case class MetricsCalculatorBuilder(private val spark: SparkSession) {

  def deequMetricsCalculator(maxMetricsPerQuery: Int, next: MetricsCalculator): MetricsCalculator =
    DeequMetricsCalculator(
      spark.sessionState.sqlParser,
      MetricsQueryParser(),
      maxMetricsPerQuery,
      next = Option(next),
    )

  def specialCasesCalculator(): MetricsCalculator =
    SpecialCasesCalculator()

  def metricsNotifierProxy(next: MetricsCalculator): MetricsCalculator =
    impl.MetricsNotifierProxy(next)

}
