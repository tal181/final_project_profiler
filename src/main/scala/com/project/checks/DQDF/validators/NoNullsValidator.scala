package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame

class NoNullsValidator(validityRecord: ValidityRecord, organizer: ValidatorOperationOrganizer)  {
  def check(data: DataFrame, columnName: Option[String]): Double = {
    val column = columnName.get
    data.select(column).where(s"$column is null").count().toDouble
  }
}