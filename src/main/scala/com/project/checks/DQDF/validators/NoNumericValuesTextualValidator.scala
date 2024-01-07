package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame

class NoNumericValuesTextualValidator(validityRecord: ValidityRecord)  {
  def check(data: DataFrame, columnName: Option[String]): Double = {
    val column = columnName.get
    data.select(column).where(s"BIGINT($column) IS NOT NULL AND DOUBLE($column) = BIGINT($column)").count().toDouble
  }
}
