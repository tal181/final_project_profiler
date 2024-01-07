package com.project.checks

object ChecksUtils {
  def isSatisfiedExpression(threshold: String, expressionResult: Double, operator: String) = {
    val thresholdToBigDecimal = BigDecimal(threshold).doubleValue()
    operator match {
      case ">" => expressionResult > thresholdToBigDecimal
      case ">=" => expressionResult >= thresholdToBigDecimal
      case "<" => expressionResult < thresholdToBigDecimal
      case "<=" => expressionResult <= thresholdToBigDecimal
      case "=" => expressionResult == thresholdToBigDecimal
      case "==" => expressionResult == thresholdToBigDecimal
      case _ => false
    }
  }
}
