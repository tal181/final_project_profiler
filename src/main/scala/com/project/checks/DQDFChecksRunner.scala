package com.project.checks

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.catalog.DataframeCatalog
import com.project.checks.DQDF.components.{DataSourceSelector, ValidatorOperationOrganizer, ValidatorSetIdentifier}
import com.project.checks.DQDF.validators.Validator
import com.project.checks.configuration.{CommandLineArgs, MetricsCalculatorBuilder}
import com.project.checks.deequ.Profiler
import com.project.checks.domain.{CheckResult, Dataset, DeequCheckConfig, MetricResult, Schema, validatorConfig}
import com.project.checks.repository.reader.impl.SourceReader
import com.project.checks.repository.writer.impl.StorageHandlerImpl
import com.project.checks.utils.{SchemaParser, SparkSessionFactory, Utils}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

case class DQDFChecksRunner(cli: CommandLineArgs) extends ChecksRunner {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def validate(res: Double, checkConfig: validatorConfig): CheckResult = {

    var status = false
    if (res != null) {
      status = ChecksUtils.isSatisfiedExpression(checkConfig.threshold, res, checkConfig.operator)
    }
    if (status) {
      logger.debug(s"Check name is ${checkConfig.description} with status ${status}")
    }
    else {
      logger.debug(s"Check name is ${checkConfig.description} with status ${status}" +
        s" actual value is ${res}, operator is ${checkConfig.operator}, threshold is ${checkConfig.threshold}")

    }
    CheckResult(checkConfig.description, status)

  }

  def runChecks(): Seq[CheckResult] = {
    implicit val spark = SparkSessionFactory.configureSpark()

    logger.debug("Reading source table")
    val table = DataSourceSelector.read(cli)
    val data = table.get.datum

    val catalog = DataframeCatalog(cli)
    catalog.init()

    val organizer = ValidatorOperationOrganizer(catalog)
    organizer.prep(data)

    val validatorSetIdentifier = new ValidatorSetIdentifier(catalog, organizer, cli)

    val validatorsMap = validatorSetIdentifier.getValidators()

    val checksEvaluated = validatorsMap.map(kv => {
      val name = kv._1
      val value = kv._2.check(data)
      val config = kv._2.checkConfig.copy(description = name)
      val result = validate(value, config)


      logger.debug(s"check name is $name, result is ${result}")
      result
    }).toSeq

    logger.debug(s"Created ${checksEvaluated.size} checks")
    val storageHandler = StorageHandlerImpl(cli.dataSetOutputDqDFPath, spark)
    storageHandler.storeChecks(checksEvaluated)

    checksEvaluated
  }
}

