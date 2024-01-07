package com.project.checks.domain

import scala.util.Try

case class Metric(query: Query, metricName: String = "", column: String, definitionName: String, metricType: String, types: Seq[String])

case class Query(value: String, ast: Option[QueryAst] = None)

case class QueryAst(aggFunction: String, columnExpr: String, condition: Option[String] = None)

case class MetricResult(metric: Metric, value: Try[String])

case class Schema(column: String, columnType: String)
