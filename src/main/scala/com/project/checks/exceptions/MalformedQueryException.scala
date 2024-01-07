package com.project.checks.exceptions

case class MalformedQueryException(query: String)
  extends RuntimeException(
    s"output dataframe does not meet the required standard. Your query must output two columns," +
      s" one must be named 'metric_name'. Fix your query: \n ${query}"
  )
