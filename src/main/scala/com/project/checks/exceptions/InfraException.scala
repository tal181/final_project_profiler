package com.project.checks.exceptions

abstract class InfraException(message: String, cause: Throwable)
  extends RuntimeException(s"An infra error occurred: ${message}", cause)
