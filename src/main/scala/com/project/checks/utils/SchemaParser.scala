package com.project.checks.utils

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.domain.Schema
import com.typesafe.config.ConfigFactory

import java.io.File
import collection.JavaConverters._

object SchemaParser {
  def parseSchema(cli: CommandLineArgs): Seq[Schema] = {
    val config = ConfigFactory.parseFile(new File(cli.fileSchemaConfigPath))

    val list = config.getConfigList("schema").asScala

    list.map(item => {
      val columName = item.getString("columnName")
      val columType = item.getString("columnType")

      Schema(columName, columType)
    })
  }
}
