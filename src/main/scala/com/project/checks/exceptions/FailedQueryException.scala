package com.project.checks.exceptions

final case class FailedQueryException(query: String, e: Exception)
  extends RuntimeException(
    s"Failed to calculate metric for query $query"
  )
