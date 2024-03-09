package com.project

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.domain.Strategy
import com.project.checks.utils.Utils
import com.project.checks.{DQDFChecksRunner, DeequChecksRunner, SqlChecksRunner}
import org.apache.commons.lang.NotImplementedException
import org.slf4j.{Logger, LoggerFactory}

object App {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val cli = new CommandLineArgs(args)

    run(cli, Strategy.Deequ)
    //    run(cli, Strategy.Sql)
    //    run(cli, Strategy.DQDF)

    VisualizationComponent.main(args)
  }

  def run(cli: CommandLineArgs, strategy: String) = {
    logger.debug(s"strategy is $strategy")
    strategy match {
      case Strategy.Sql => {
        val checksRunner = SqlChecksRunner(cli)
        Utils.time(strategy, checksRunner.runChecks)
      }
      case Strategy.DQDF => {
        val checksRunner = DQDFChecksRunner(cli)
        Utils.time(strategy, checksRunner.runChecks)
      }
      case Strategy.Deequ => {
        val checksRunner = DeequChecksRunner(cli)
        Utils.time(strategy, checksRunner.runChecks)
      }
    }
  }
}
