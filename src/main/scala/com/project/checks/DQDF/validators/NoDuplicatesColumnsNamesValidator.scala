package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame

class NoDuplicatesColumnsNamesValidator(validityRecord: ValidityRecord, organizer: ValidatorOperationOrganizer) {
  def check(data: DataFrame, columnName: Option[String]): Double = {
    if (data.schema.fields.distinct.size == data.schema.size) {
      return 0
    }
    1

  }
}
