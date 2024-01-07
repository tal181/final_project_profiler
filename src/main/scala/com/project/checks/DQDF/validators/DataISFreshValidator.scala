package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

class DataISFreshValidator(validityRecord: ValidityRecord, organizer: ValidatorOperationOrganizer)  {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def check(data: DataFrame, columnName: Option[String]): Double = {
    val column = columnName.get
    val value = data.selectExpr(s"max(datediff($column, current_date()))").collect().head.getInt(0).toDouble
    value
  }
}