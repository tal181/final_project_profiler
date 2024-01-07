package com.project.checks.sql

import com.project.checks.domain.Constants
import Constants._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.DataTypes.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/**
  * @param spark              The spark session
  */
case class SqlRunner(spark: SparkSession) extends Serializable  {
  import spark.implicits._
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  type SqlResult = Try[String]

  def runSql(query: String): SqlResult = Try {
    spark.sparkContext.setJobDescription(s"$query")

    val value = spark
      .sql(query)
      .transform(asQueryResult)
      .head() //there should be exact one result per query
      .value

    value
    //TODO we must run count rows check to prevent queries with groupBy
  }

  private def asQueryResult(df: DataFrame): Dataset[QueryResult] = {
    df
      .withColumn(MetricValueColumn, col(Constants.MetricNameColumn).cast(StringType))
      .as[QueryResult]
  }


}

case class QueryResult(value: String)
