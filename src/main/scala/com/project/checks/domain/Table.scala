package com.project.checks.domain

import org.apache.spark.sql.DataFrame

case class Table(name: String, datum: DataFrame, schema: Seq[Schema])


