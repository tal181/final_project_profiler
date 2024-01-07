package com.project.checks.deequ.calculator

import com.project.checks.domain.{Metric, MetricResult, Table}
import org.slf4j.{Logger, LoggerFactory}

import java.math.RoundingMode
import java.text.DecimalFormat

trait MetricsCalculator  {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  //Chain Of Responsibility
  protected val next: Option[MetricsCalculator] = None

  def calculate(table: Table, metrics: Seq[Metric]): Seq[MetricResult]

  protected def runNext(table: Table, metrics: Seq[Metric]): Seq[MetricResult] = next match {
    case Some(nextCalculator) => nextCalculator.calculate(table, metrics)
    case None =>
      if (metrics.nonEmpty)
        logger.warn(s"The next calculator isn't defined #${metrics.size} metrics won't be calculated")
      Seq.empty
  }

  protected def invert[K, V](map: Map[K, V]): Map[V, Seq[K]] =
    map.groupBy { case (_, value) => value }.mapValues(_.keys.toSeq)

}

object MetricsCalculator {
  /*
    remove zero after the point
    0.0 -> 0
    9.0 -> 9
    1325.3434 -> 1325.3434
    1325.0000 -> 1325
   */
  def convertToString(number: Double): String = {
    val formatter = new DecimalFormat("#.##################");
    //formatter.setRoundingMode(RoundingMode.DOWN)
    formatter.format(number)
  }
}
