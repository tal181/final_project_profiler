package com.project.checks.DQDF.catalog

import com.project.checks.DQDF.validators.Validator
import com.project.checks.configuration.CommandLineArgs
import com.project.checks.domain.{Schema, validatorConfig}
import com.project.checks.utils.{SchemaParser, Utils}
import com.typesafe.config.ConfigFactory

//A catalog class contains statistical information
//  about the underlying data, its validators, each validator’s
//independent computations, and the data quality information. The
//dataframe catalog also contains a list of validator-specific information
//  (validator catalog).

case class DataframeCatalog(cli: CommandLineArgs) {

  val map = scala.collection.mutable.Map[String, Any]()

  def getAttribute(key: String): Any = {
    map.get(key).get
  }

  def init() = {
    val schema = SchemaParser.parseSchema(cli)
    map += "schema" -> schema
  }
}
