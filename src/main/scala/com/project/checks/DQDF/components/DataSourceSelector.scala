package com.project.checks.DQDF.components

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.domain.{Dataset, Schema}
import com.project.checks.repository.reader.impl.SourceReader
import com.project.checks.utils.SchemaParser
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object DataSourceSelector {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def read(cli: CommandLineArgs)(implicit spark: SparkSession) = {

    logger.debug(s"Starting profiling")
    val dataset = Dataset("dataset", cli.dataSetPath)

    logger.debug("Loading schema")
    val schema: Seq[Schema] = SchemaParser.parseSchema(cli)

    val sourceReader = SourceReader(cli.dataSetPath, spark)

    sourceReader
      .read(dataset, schema)
  }
}
