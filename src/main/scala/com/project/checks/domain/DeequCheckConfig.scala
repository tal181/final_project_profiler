package com.project.checks.domain

import scala.util.Try

case class validatorConfig(name: String, description:String, operator: String, threshold: String, types: Seq[String])
case class DeequCheckConfig(checkName: String, expression: String, operator: String, threshold: String)

case class SqlCheckConfig(query: Query, checkName: String, column: String, metricType: String, types: Seq[String], operator:String, threshold:String)

case class CheckEvaluation(checkName: String, expression: Try[String])

case class CheckResult(checkName: String, status: Boolean)

