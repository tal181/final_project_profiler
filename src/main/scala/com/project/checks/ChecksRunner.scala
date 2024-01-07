package com.project.checks

import com.project.checks.domain.CheckResult

trait ChecksRunner {
  def runChecks(): Seq[CheckResult]
}
