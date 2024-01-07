package com.project.checks

import com.project.checks.configuration.{CommandLineArgs, MetricsCalculatorBuilder}
import com.project.checks.domain.{CheckEvaluation, CheckResult, DeequCheckConfig, MetricResult, Query, Schema, SqlCheckConfig}
import com.project.checks.repository.reader.impl.SourceReader
import com.project.checks.repository.writer.impl.StorageHandlerImpl
import com.project.checks.sql.Profiler
import com.project.checks.utils.{SchemaParser, SparkSessionFactory, Utils}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

case class SqlChecksRunner(cli: CommandLineArgs) extends ChecksRunner {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def runChecks(): Seq[CheckResult] = {
    implicit val spark = SparkSessionFactory.configureSpark()

    logger.debug("Loading schema")
    val schema: Seq[Schema] = SchemaParser.parseSchema(cli)

    logger.debug("Loading Checks")
    val checksConfig: Seq[SqlCheckConfig] = parseChecks(schema)
    logger.debug(s"Created ${checksConfig.size} checks")

    val checkResults = getCheckEvaluations(checksConfig)
    logger.debug(s"checkResults for $checkResults")


    val checksEvaluated = evaluateResults(checkResults, checksConfig)

    val storageHandler = StorageHandlerImpl(cli.dataSetOutputSqlPath, spark)
    storageHandler.storeChecks(checksEvaluated)

    checksEvaluated
  }

  private def getCheckEvaluations(checks: Seq[SqlCheckConfig])(implicit spark: SparkSession): Seq[CheckEvaluation] = {

    val sourceReader = SourceReader(cli.dataSetPath, spark)

    val profiler =
      Profiler(sourceReader, checks)

    val checkResult = profiler.run(cli)

    checkResult
  }

  private def parseChecks(schema: Seq[Schema]): Seq[SqlCheckConfig] = {

    import collection.JavaConverters._
    val checksConfig = ConfigFactory.load(cli.checksConfigSqlSqlPath)

    val list = checksConfig.getConfigList("checks").asScala

    list.flatMap(item => {
      val query = Utils.getAttribute(item, "query")

      val checkName = Utils.getAttribute(item, "checkName")
      val metricType = Utils.getAttribute(item, "metricType")
      val types = item.getList("types").asScala.map(lItem => lItem.unwrapped().asInstanceOf[String])
      val operator = Utils.getAttribute(item, "operator")
      val threshold = Utils.getAttribute(item, "threshold")
      generateMetricsAccordingToSchema(query, checkName, metricType, types, schema, operator, threshold)

    })

  }

  private def generateMetricsAccordingToSchema(query: String, checkName: String, metricType: String, types: Seq[String], schema: Seq[Schema], operator: String, threshold: String): Seq[SqlCheckConfig] = {
    if (metricType.equals("Dataset")) {
      val schemaString = schema.map(column => column.column).mkString(",")
      return Seq(SqlCheckConfig(Query(query), checkName, schemaString, metricType, types, operator, threshold))
    }

    schema.filter(column => types.contains(column.columnType) || types.contains("any"))
      .map(column => {
        val checkNameNameToReplace = checkName.replace("${column}", column.column)
        SqlCheckConfig(Query(query), checkNameNameToReplace, column.column, metricType, types, operator, threshold)
      })

  }

  private def evaluateResults(checksToEvaluate: Seq[CheckEvaluation], checksConfig: Seq[SqlCheckConfig]): Seq[CheckResult] = {

    val checksConfigMap = checksConfig.map(check => check.checkName -> check).toMap
    checksToEvaluate.flatMap(check => {
      try {
        val checkConfig = checksConfigMap.get(check.checkName).get
        var res = check.expression.get

        var status = false
        if(res != null){
          status = ChecksUtils.isSatisfiedExpression(checkConfig.threshold, res.toDouble, checkConfig.operator)
        }
        if (status) {
          logger.debug(s"Check name is ${check.checkName} with status ${status}")
        }
        else {
          logger.debug(s"Check name is ${check.checkName} with status ${status}" +
            s" actual value is ${res}, operator is ${checkConfig.operator}, threshold is ${checkConfig.threshold}")

        }
        Some(CheckResult(check.checkName, status))
      }
      catch {
        case e: scala.tools.reflect.ToolBoxError => {
          logger.error(s"Failed to calculate check ${check.checkName}, error: ${e.getMessage}")
          None
        }
      }
    })
  }
}

