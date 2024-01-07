package com.project.checks.sql

import com.project.checks.domain.{Metric, SqlCheckConfig}
import com.project.checks.utils.StringBuilder
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

object QueryFactory  {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  def buildQuery(table: String, check: SqlCheckConfig): String = {
    val fixedTableName = StringBuilder.escapeTableName(table)

    val tableName = "{table}"
    val queryParam = "{column}"

    val fixedQuery = check.query.value
      .replaceAll(Regex.quote(tableName), fixedTableName)
      .replaceAll(Regex.quote(queryParam), check.column)

    fixedQuery
  }
}
