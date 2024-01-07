package com.project.checks.sql

import com.project.checks.deequ.calculator.MetricsCalculator
import com.project.checks.domain.{CheckEvaluation, CheckResult, Metric, MetricResult, SqlCheckConfig, Table}
import com.project.checks.utils.StringBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

case class QueryPerMetricCalculator(sqlRunner: SqlRunner)   {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
   def calculate(table: Table, checks: Seq[SqlCheckConfig]): Seq[CheckEvaluation] = {
    logger.debug(s"[Start] Calculating the checks")

    //this is necessary to prevent None.get error when calling reduce on an empty collections
    //as we do on line 35
    if (checks.isEmpty) {
      logger.debug("There are no checks left, skipping")
      logger.debug(s"[End] Calculating the metrics")
      return Seq.empty
    }

    logger.debug(s"Going to calculate ${checks.size} checks")

    def buildAndRunQuery(check: SqlCheckConfig): CheckEvaluation = {
      Try(QueryFactory.buildQuery(table.name, check))
        .map(sqlRunner.runSql)
        .map(toMetricResult(check))
        .fold(onErrorRecoverWithFailedMetricResult(check), identity)
    }

    val fixedTableName = StringBuilder.escapeTableName(table.name)
    table.datum.createOrReplaceTempView(fixedTableName)
    val results = checks.par.map(buildAndRunQuery).seq

    logger.debug(s"[End] Calculating the checks")
    results
  }

  private def toMetricResult(check: SqlCheckConfig)(queryResult: Try[String]) = {
    log(check, queryResult)
    CheckEvaluation(check.checkName, queryResult)
  }

  private def onErrorRecoverWithFailedMetricResult(check: SqlCheckConfig)(ex: Throwable) = {
    logger.debug(
      s"Metric failed for unexpected reason $ex context: " +
        s"(column: ${check.column}, query: ${check.query.value})")

    CheckEvaluation(check.checkName, Failure(ex))
  }

  private def log(check: SqlCheckConfig, queryResult: sqlRunner.SqlResult): Unit = {
    queryResult match {
      case Failure(ex) =>
        logger.debug(
          s"The check has failed " +
            s"(column: ${check.column}, query: ${check.query.value}) is $ex")
      case Success(value) =>
        logger.debug(
          s"The result value for the check " +
            s"(column: ${check.column}, query: ${check.query.value}) is $value")
    }
  }
}
