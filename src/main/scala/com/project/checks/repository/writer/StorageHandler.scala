package com.project.checks.repository.writer

import com.project.checks.domain.{CheckResult, DeequCheckConfig, MetricResult}

trait StorageHandler {

  def storeMetrics(results: Seq[MetricResult]): Unit

  def storeChecks(results: Seq[CheckResult]): Unit

}
