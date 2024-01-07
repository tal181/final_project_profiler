package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import org.apache.spark.sql.DataFrame

class NoNumericValuesSmaller0Validator(validityRecord: ValidityRecord)  {
  def check(data: DataFrame, columnName: Option[String]): Double = {
    val column = columnName.get
    data.select(column).where(s"$column <=0 or $column <=0.0").count().toDouble
  }
}
