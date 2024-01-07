package com.project.checks.DQDF.validators

import com.project.checks.ChecksUtils
import com.project.checks.DQDF.ValidityRecord
import com.project.checks.DQDF.components.ValidatorOperationOrganizer
import com.project.checks.domain.{CheckResult, validatorConfig}
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.reflect.runtime.{universe => ru}

case class Validator(className: String, columnName: Option[String], description: String, checkConfig: validatorConfig, record: ValidityRecord,organizer: ValidatorOperationOrganizer) {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def check(data: DataFrame): Double = {

    val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol: ru.ClassSymbol = mirror.staticClass(className)
    val classMirror = mirror.reflectClass(classSymbol)

    val methodSymbol = classSymbol.info.decl(ru.TermName("check")).asMethod

    val consMethodSymbol = classSymbol.primaryConstructor.asMethod

    val consMethodMirror = classMirror.reflectConstructor(consMethodSymbol)

    val result = consMethodMirror.apply(record, organizer)

    val instanceMirror = mirror.reflect(result)

    val method = instanceMirror.reflectMethod(methodSymbol)

    val resultMethod = method.apply(data, columnName).asInstanceOf[Double]

    resultMethod

  }
}

