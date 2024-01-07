package com.project.checks.exceptions

case class MetricValueIsNullOrIllegal()
  extends RuntimeException("The metric value was null or illegal. This may happen when aggregating over zero rows " +
    "or if illegal parameters were passed to a function")
