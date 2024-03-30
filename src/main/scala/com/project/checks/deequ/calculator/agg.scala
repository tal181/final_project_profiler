package com.project.checks.deequ.calculator

object agg {
  sealed trait AggFunction
  case object Count extends AggFunction
  case object CountDistinct extends AggFunction
  case object Min extends AggFunction
  case object Max extends AggFunction
  case object StdDev extends AggFunction
  case object Sum extends AggFunction
  case object Avg extends AggFunction
  case object Distinctness extends AggFunction
}
