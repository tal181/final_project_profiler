package com.project.checks.deequ.calculator.impl

import com.project.checks.deequ.calculator.MetricsCalculator
import com.project.checks.domain.{Metric, MetricResult, Table}

import scala.util.Failure

case class MetricsNotifierProxy(underlying: MetricsCalculator) extends MetricsCalculator {

  require(underlying != null)

  override protected val next: Option[MetricsCalculator] = Option(underlying)

  def calculate(table: Table, metrics: Seq[Metric]): Seq[MetricResult] = {

    val results = next.get.calculate(table, metrics)

    results
  }
}
