package com.project.checks.utils

//todo merge with commons
/**
  * Util methods for building different reusable strings.
  */
object StringBuilder  {

  /**
    * Returns an escaped name.
    *
    * @param tableName  The table name to escape
    * @return An escape table name
    */
  def escapeTableName(tableName: String): String =
    if (tableName.contains('`'))
      tableName
    else
      s"`${tableName.replace(".", "`.`")}`"

}
