package com.project.checks.DQDF.components

import com.project.checks.DQDF.catalog.DataframeCatalog
import com.project.checks.DQDF.validators
import com.project.checks.DQDF.validators.Validator
import com.project.checks.configuration.CommandLineArgs
import com.project.checks.domain.{Schema, validatorConfig}
import com.project.checks.utils.Utils
import com.typesafe.config.ConfigFactory

//selects a predefined
//set of validators based on the type of the underlying data
class ValidatorSetIdentifier(catalog: DataframeCatalog, operationOrganizer: ValidatorOperationOrganizer,cli: CommandLineArgs) {
  def createValidator(config: validatorConfig): Map[String, Validator] = {
    val types = config.types
    if (types.isEmpty) {
      val description = config.description
      Map(description -> validators.Validator(config.name, None, description, config, null, operationOrganizer))
    }
    else {
      val schema = catalog.getAttribute("schema").asInstanceOf[Seq[Schema]]
      schema.filter(column => types.contains(column.columnType) || types.contains("any"))
        .map(column => {
          val description = config.description.replace("${column}", column.column)
          description  -> validators.Validator(config.name, Some(column.column), description,config, null, operationOrganizer)
        }).toMap
    }
  }

  def getValidators(): Map[String, Validator] = {
    val validatorsConfig = parseValidators()

    val validators = validatorsConfig.map(config => createValidator(config)).flatten.toMap
    validators
  }

  private def parseValidators(): Seq[validatorConfig] = {

    val checksConfig = ConfigFactory.load(cli.checksConfigDQDFPath)
    import collection.JavaConverters._
    val list = checksConfig.getConfigList("validators").asScala

    list.map(item => {

      val operator = Utils.getAttribute(item, "operator")
      val threshold = Utils.getAttribute(item, "threshold")
      val validatorName = Utils.getAttribute(item, "name")
      val description = Utils.getAttribute(item, "description")

      val types = item.getList("types").asScala.map(lItem => lItem.unwrapped().asInstanceOf[String])

      val fixedName = s"com.project.checks.DQDF.validators.$validatorName"
      validatorConfig(fixedName, description, operator, threshold, types)
    })
  }
}
