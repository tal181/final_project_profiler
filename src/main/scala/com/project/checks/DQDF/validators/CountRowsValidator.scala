package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.catalog.DataframeCatalog
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame

class CountRowsValidator(validityRecord: ValidityRecord, organizer: ValidatorOperationOrganizer) {
  def check(data: DataFrame, columnName: Option[String]): Double = {
    val value = organizer.getValue("countRows")
    value
  }
}

