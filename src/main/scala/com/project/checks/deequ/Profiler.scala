package com.project.checks.deequ

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.deequ.calculator.{Calculators, MetricsCalculator}
import com.project.checks.domain.{Dataset, Metric, MetricResult, Query, Schema, Table}
import com.project.checks.repository.reader.impl.SourceReader
import com.project.checks.utils.SchemaParser
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

case class Profiler(sourceReader: SourceReader,
                    metricsCalculator: MetricsCalculator,
                    metricsConfig: Config) {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def run(cli: CommandLineArgs): Seq[MetricResult] = {

    logger.debug(s"Starting profiling")
    val dataset = Dataset("dataset", cli.dataSetPath)

    logger.debug("Loading schema")
    val schema: Seq[Schema] = SchemaParser.parseSchema(cli)

    logger.debug("Loading metrics")
    val metrics: Seq[Metric] = parseMetrics(schema)
    logger.debug(s"Created ${metrics.size} metrics")

    logger.debug("Reading source table")
    sourceReader
      .read(dataset, schema)
      .fold(
        onReadTableFailure(metrics),
        onSuccessCalculateMetrics(metrics)
      )
  }

  private def parseMetrics(schema: Seq[Schema]): Seq[Metric] = {

    import collection.JavaConverters._
    val list = metricsConfig.getConfigList("metrics").asScala

    list.flatMap(item => {
      val query = getAttribute(item, "query")

      val metricName = getAttribute(item, "metricName")
      val definitionName = getAttribute(item, "definitionName")
      val metricType = getAttribute(item, "metricType")
      val types = item.getList("types").asScala.map(lItem => lItem.unwrapped().asInstanceOf[String])

      generateMetricsAccordingToSchema(query, metricName, definitionName, metricType, types, schema)

    })

  }

  private def getAttribute(item: Config, attributeName: String): String = {
    try {
      item.getString(attributeName)
    }
    catch {
      case e: Exception => {
        ""
      }
    }
  }

  private def generateMetricsAccordingToSchema(query: String, metricName: String, definitionName: String, metricType: String, types: Seq[String], schema: Seq[Schema]): Seq[Metric] = {
    if (metricType.equals("Dataset")) {
      val schemaString = schema.map(column => column.column).mkString(",")
      return Seq(Metric(Query(query), metricName, schemaString, definitionName, metricType, types))
    }

    schema.filter(column => types.contains(column.columnType) || types.contains("any"))
      .map(column => {
        val metricNameToReplace = metricName.replace("${column}", column.column) //for spark sql checks
        Metric(Query(query), metricNameToReplace, column.column, definitionName, metricType, types)
      })

  }


  private def onSuccessCalculateMetrics(metrics: Seq[Metric])(table: Table): Seq[MetricResult] = {
    logger.debug("Calculating metrics")
    metricsCalculator.calculate(table, metrics)
  }

  private def onReadTableFailure(metrics: Seq[Metric])(ex: Throwable): Seq[MetricResult] = {
    logger.error(s"Failed reading the source table error: ${ex.getMessage}")
    ex match {
      case _ => Calculators.onErrorMarkAllMetricsAsFailed(metrics)(ex)
    }
  }

}
