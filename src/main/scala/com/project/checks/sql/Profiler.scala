package com.project.checks.sql

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.deequ.calculator.Calculators
import com.project.checks.domain.{CheckEvaluation, Dataset, Schema, SqlCheckConfig, Table}
import com.project.checks.repository.reader.impl.SourceReader
import com.project.checks.utils.{SchemaParser, SparkSessionFactory}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Seq

case class Profiler(sourceReader: SourceReader, checks: Seq[SqlCheckConfig]) {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def run(cli: CommandLineArgs): Seq[CheckEvaluation] = {
    logger.debug(s"Starting profiling")
    val dataset = Dataset("dataset", cli.dataSetPath)

    logger.debug("Loading schema")
    val schema: Seq[Schema] = SchemaParser.parseSchema(cli)

    logger.debug("Loading Checks")
    logger.debug(s"Created ${checks.size} checks")

    logger.debug("Reading source table")
     sourceReader
      .read(dataset, schema)
      .fold(
        onReadTableFailure(checks),
        onSuccessCalculateMetrics(checks)
      )
  }

  private def onSuccessCalculateMetrics(checks: Seq[SqlCheckConfig])(table: Table): Seq[CheckEvaluation] = {
    implicit val spark = SparkSessionFactory.configureSpark()
    logger.debug("Calculating metrics")
    val runner = QueryPerMetricCalculator(SqlRunner(spark))
    runner.calculate(table,checks )
  }

  private def onReadTableFailure(checks: Seq[SqlCheckConfig])(ex: Throwable): Seq[CheckEvaluation] = {
    logger.error(s"Failed reading the source table error: ${ex.getMessage}")
    ex match {
      case _ => Calculators.onErrorMarkAllChecksAsFailed(checks)(ex)
    }
  }
}