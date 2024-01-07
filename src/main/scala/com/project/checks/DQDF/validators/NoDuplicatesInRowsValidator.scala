package com.project.checks.DQDF.validators

import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import org.apache.spark.sql.DataFrame

class NoDuplicatesInRowsValidator(validityRecord: ValidityRecord, organizer: ValidatorOperationOrganizer)  {
  def check(data: DataFrame, columnName: Option[String]): Double = {

   val value = data
     .select("*")
     .distinct()
     .count().toDouble

    organizer.getValue("countRows") - value
  }
}