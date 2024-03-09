package com.project.checks.configuration

import com.project.checks.domain.Strategy
import org.rogach.scallop._
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime

/**
  * Parse the command line arguments.
  *
  * @param args  The command line arguments we should parse.
  */
class CommandLineArgs(args: Seq[String]) extends ScallopConf(args)  {
  val Logger = LoggerFactory.getLogger(this.getClass)

  private val strategyArg  = opt[String]("strategy", required = true, default = Some(Strategy.Sql))
  private val dataSetPathArg = opt[String]("dataSetPath", required = true, default = Some("/Users/tsharon/IdeaProjects/final_project/src/main/resources/text.csv"))
  private val dataSetOutputPathSqlArg = opt[String]("dataSetOutputPathSql", required = true, default = Some("/Users/tsharon/IdeaProjects/final_project/src/main/resources/sql"))
  private val dataSetOutputPathDeequArg = opt[String]("dataSetOutputPathDeequ", required = true, default = Some("/Users/tsharon/IdeaProjects/final_project/src/main/resources/deequ"))
  private val dataSetOutputPathDQDFArg = opt[String]("dataSetOutputPathDQDF", required = true, default = Some("/Users/tsharon/IdeaProjects/final_project/src/main/resources/dqdf"))


  private val maxMetricsPerQueryArg = opt[Int]("maxMetricsPerQuery", required = false, default = Some(2000))
  private val sparkConfigPathArg = opt[String]("sparkConfigPath", required = true, default = Some("spark-local.properties"))

  private val checksConfigSqlSqlPathArg = opt[String]("metricsConfigSqlPath", required = true, default = Some("checksConfigSql.json"))

  private val checksConfigDeequPathArg = opt[String]("checksConfigDeequPathArg", required = true, default = Some("checksConfigDeequ.json"))

  private val checksConfigDQDFPathArg = opt[String]("checksConfigDQDFPathArg", required = true, default = Some("checksConfigDQDF.json"))

  private val fileSchemaConfigPathArg = opt[String]("fileSchemaConfigPathArg", required = true, default = Some("fileSchemaConfig.json"))


  verify

  val strategy: String = {
    val strategy: String = strategyArg()
    Logger.debug(s"strategy $strategyArg")
    strategy
  }

  val fileSchemaConfigPath: String = {
    val fileSchemaConfigPath: String = fileSchemaConfigPathArg()
    Logger.debug(s"fileSchemaConfigPath $fileSchemaConfigPathArg")
    fileSchemaConfigPath
  }

  val checksConfigDQDFPath: String = {
    val checksConfigDQDFPath: String = checksConfigDQDFPathArg()
    Logger.debug(s"checksConfigDeequPathArg $checksConfigDQDFPathArg")
    checksConfigDQDFPath
  }

  val checksConfigDeequPath: String = {
    val checksConfigDeequPath: String = checksConfigDeequPathArg()
    Logger.debug(s"checksConfigDeequPathArg $checksConfigDeequPathArg")
    checksConfigDeequPath
  }

  val checksConfigSqlSqlPath: String = {
    val checksConfigSqlPath: String = checksConfigSqlSqlPathArg()
    Logger.debug(s"checksConfigSqlSqlPathArg $checksConfigSqlSqlPathArg")
    checksConfigSqlPath
  }

  val dataSetPath: String = {
    val dataSetPath: String = dataSetPathArg()
    Logger.debug(s"dataSetPath $dataSetPath")
    dataSetPath
  }

  val dataSetOutputDeequPath: String = {
    val dataSetPath: String = dataSetOutputPathDeequArg()
    Logger.debug(s"dataSetOutputPathArg $dataSetPath")
    dataSetPath
  }

  val dataSetOutputSqlPath: String = {
    val dataSetPath: String = dataSetOutputPathSqlArg()
    Logger.debug(s"dataSetOutputPathArg $dataSetPath")
    dataSetPath
  }

  val dataSetOutputDqDFPath: String = {
    val dataSetPath: String = dataSetOutputPathDQDFArg()
    Logger.debug(s"dataSetOutputPathArg $dataSetPath")
    dataSetPath
  }

  val maxMetricsPerQuery: Int = {
    val maxMetricsPerQuery = maxMetricsPerQueryArg()
    Logger.debug(s"maxMetricsPerQuery is $maxMetricsPerQuery")
    maxMetricsPerQuery
  }

  val sparkConfigPath: String = {
    val confPath = sparkConfigPathArg()
    Logger.debug(s"confPath is $confPath")
    confPath
  }
}
