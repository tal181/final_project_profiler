package com.project.checks.domain

import com.project.checks.deequ.calculator.ParsedQuery

case class MetricWithParsedQuery(metric: Metric, parsedQuery: ParsedQuery)
