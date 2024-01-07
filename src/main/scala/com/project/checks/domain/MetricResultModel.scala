package com.project.checks.domain

case class MetricResultModel(dataSetId: Int,
                             metricId: Int,
                             value: String,
                             dataTime: String,
                             processingTime: String,
                             createdAt: Option[String], // in UTC ISO format, 2020-11-04T20:46:28Z
                             dqRunId: Int)
