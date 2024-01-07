package com.project.checks.repository.reader.impl

import com.project.checks.domain.{Dataset, Schema, Table}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

case class SourceReader(dataSetPath: String, spark: SparkSession)  {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  def read(dataset: Dataset, schema: Seq[Schema]): Try[Table] = Try {
    val datum = spark.read.option("header",true).csv(dataSetPath)
    Table(dataset.name, datum, schema)
  }
}
