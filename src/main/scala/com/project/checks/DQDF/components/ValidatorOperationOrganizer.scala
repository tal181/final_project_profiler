package com.project.checks.DQDF.components

import com.project.checks.DQDF.catalog.DataframeCatalog
import com.project.checks.domain.Schema
import org.apache.spark.sql.DataFrame

//rearranges
//the identified validators and extract shared computations to
//pre-execute
case class ValidatorOperationOrganizer(catalog: DataframeCatalog) {

  val map = scala.collection.mutable.Map[String,Double]()

  def getValue(key: String): Double = {
    map.get(key).get
  }
  def prep(df: DataFrame): scala.collection.mutable.Map[String, Double] = {
    val schema = catalog.getAttribute("schema").asInstanceOf[Seq[Schema]]

    schema.map(item => {
      val colName = item.column

      val numRows = df
        .select(colName)
        .distinct()
        .count()

      map += s"$colName" -> numRows.toDouble

    })

    map += "countRows" ->  df.count()
  }
}
