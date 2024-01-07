package com.project.checks

import com.project.checks.configuration.{CommandLineArgs, MetricsCalculatorBuilder}
import com.project.checks.deequ.Profiler
import com.project.checks.domain.{CheckEvaluation, CheckResult, DeequCheckConfig, MetricResult, Schema}
import com.project.checks.repository.reader.impl.SourceReader
import com.project.checks.repository.writer.impl.StorageHandlerImpl
import com.project.checks.utils.{SchemaParser, SparkSessionFactory, Utils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.Try

case class DeequChecksRunner(cli: CommandLineArgs) extends ChecksRunner {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def runChecks(): Seq[CheckResult] = {
    implicit val spark = SparkSessionFactory.configureSpark()

    val metrics = getMetrics

    logger.debug(s"metrics str $metrics")

    val schema = SchemaParser.parseSchema(cli)
    val checks = parseChecks(cli, schema)

    logger.debug(s"going to calculate ${checks.size} checks")
    val checksToEvaluate = replaceMetrics(checks, metrics)

    val checksEvaluated = evaluateResults(checksToEvaluate)

    logger.debug(s"Evaluated successfully  ${checksEvaluated.size} out of ${checks.size} checks")

    val storageHandler = StorageHandlerImpl(cli.dataSetOutputDeequPath, spark)
    storageHandler.storeChecks(checksEvaluated)

    checksEvaluated
  }

  private def getMetrics()(implicit spark: SparkSession) = {


    val sourceReader = SourceReader(cli.dataSetPath, spark)


    val metricCalculatorBuilder = MetricsCalculatorBuilder(spark)
    import metricCalculatorBuilder._

    val metricsCalculator = metricsNotifierProxy(
      next = deequMetricsCalculator(
        cli.maxMetricsPerQuery, next = specialCasesCalculator,
      ))

    val metricsConfig = ConfigFactory.load(cli.metricsConfigDeequPath)

    val profiler =
      deequ.Profiler(sourceReader, metricsCalculator, metricsConfig)

    val metricResults = profiler.run(cli)
    metricResults
  }


  private def evaluateResults(checksToEvaluate: Seq[DeequCheckConfig]): Seq[CheckResult] = {
    val toolbox = currentMirror.mkToolBox()


    checksToEvaluate.flatMap(check => {
      try {
        if (check.expression.contains("N/A")) {
          Some(CheckResult(check.checkName, false))
        }
        else {
          val res = toolbox.eval(toolbox.parse(check.expression))
          val status = ChecksUtils.isSatisfiedExpression(check.threshold, res.toString.toDouble, check.operator)
          if (status) {
            logger.debug(s"Check name is ${check.checkName} with status ${status}")
          }
          else {
            logger.debug(s"Check name is ${check.checkName} with status ${status}" +
              s" actual value is ${res}, operator is ${check.operator}, threshold is ${check.threshold}")

          }
          Some(CheckResult(check.checkName, status))
        }
      }
      catch {
        case e: scala.tools.reflect.ToolBoxError => {
          logger.error(s"Failed to calculate check ${check.checkName}, error: ${e.getMessage}")
          None
        }
      }
    })
  }

  private def replaceMetrics(checks: Seq[DeequCheckConfig], metrics: Seq[MetricResult]) = {
    val metricsMap = metrics.map(metricResult => {
      if (metricResult.metric.metricType == "Dataset") {
        metricResult.metric.metricName -> Some(metricResult.value.get.toDouble)
      }
      else if (metricResult.value.get == null) {
        (metricResult.metric.metricName + "_" + metricResult.metric.column) -> None //todo unable to calc metric
      }
      else {
        (metricResult.metric.metricName + "_" + metricResult.metric.column) -> Some(metricResult.value.get.toDouble)
      }
    }).toMap

    checks.flatMap(check => {
      val expression = resolveExpression(check, metricsMap)

      Seq(DeequCheckConfig(check.checkName, expression, check.operator, check.threshold))
    })

  }

  private def resolveExpression(check: DeequCheckConfig, metricsMap: Map[String, Option[Double]]): String = {
    val parsedExpression = check.expression.
      replaceAll("\\(", "")
      .replaceAll("\\)", "")
      .split(" ")

    val res: Map[String, Option[Double]] = parsedExpression.map(subExpression => {
      val resolved = metricsMap.get(subExpression)
      subExpression -> resolved.flatten
    }).toMap

    var expression = check.expression
    res
      .map(item => {
        if (item._1.contains("-")) {
          //do nothing
        }
        else if (item._2.nonEmpty) {
          expression = expression.replace(item._1, item._2.get.toString)
        }
        else {
          expression = expression.replace(item._1, "N/A")
        }
      })

    expression
  }

  private def parseChecks(cli: CommandLineArgs, schema: Seq[Schema]): Seq[DeequCheckConfig] = {

    val checksConfig = ConfigFactory.load(cli.checksConfigDeequPath)
    import collection.JavaConverters._
    val list = checksConfig.getConfigList("checks").asScala

    list.flatMap(item => {
      val expression = Utils.getAttribute(item, "expression")

      val operator = Utils.getAttribute(item, "operator")
      val threshold = Utils.getAttribute(item, "threshold")
      val checkName = Utils.getAttribute(item, "checkName")
      val types = item.getList("types").asScala.map(lItem => lItem.unwrapped().asInstanceOf[String])

      generateChecksAccordingToSchema(checkName, expression, operator, threshold, types, schema)
    })
  }

  private def generateChecksAccordingToSchema(checkName: String, expression: String, operator: String, threshold: String, types: Seq[String], schema: Seq[Schema]): Seq[DeequCheckConfig] = {

    if (types.isEmpty) {
      Seq(DeequCheckConfig(checkName, expression, operator, threshold))
    }
    else {
      schema.filter(column => types.contains(column.columnType) || types.contains("any"))
        .map(column => {
          val checkNameToReplace = checkName.replace("${column}", column.column)
          val expressionToReplace = expression.replace("${column}", column.column)

          DeequCheckConfig(checkNameToReplace, expressionToReplace, operator, threshold)
        })
    }
  }
}

