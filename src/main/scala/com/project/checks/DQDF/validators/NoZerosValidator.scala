package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame

class NoZerosValidator(validityRecord: ValidityRecord, organizer: ValidatorOperationOrganizer)  {
  def check(data: DataFrame, columnName: Option[String]): Double = {
    val column = columnName.get
    data.select(column).where(s"$column = 0 or $column = 0.00").count().toDouble
  }
}
