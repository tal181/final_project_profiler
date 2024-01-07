package com.project.checks.utils

import com.project.checks.domain.Constants
import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

/**
  * Creates a SparkSession.
  */
object SparkSessionFactory  {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val emptySparkConf = new SparkConf()

  def configureSpark(): SparkSession = {
    SparkSessionFactory.createSession("spark-local.properties")
  }

  def createSession(sparkConfigPath: String, additionalConf: SparkConf = emptySparkConf): SparkSession = {
    val builder = SparkSession.builder().appName(Constants.Metrics)
    sessionWithStandardConfig(builder, sparkConfigPath, additionalConf)
  }

  /**
    * Creates a SparkSession, either a session used only locally if 'isLocalSpark' is true, or a session used in a
    * real spark cluster if 'isLocalSpark' is false.
    *
    * @param builder          The SparkSession.Builder with any app-specific settings already in place
    * @param sparkConfigPath  The location of the spark config file
    * @return The SparkSession with standard configuration added to the app-specific one, and master set or overridden
    */
  def sessionWithStandardConfig(builder: SparkSession.Builder,
                                sparkConfigPath: String,
                                additionalConf: SparkConf = emptySparkConf): SparkSession = {

    val props = ConfigFactory.parseResourcesAnySyntax(sparkConfigPath)
    val configs = props.entrySet().asScala

    builder
      .config(additionalConf)

    configs.foreach(item => {
      val value = item.getValue.unwrapped().toString
      builder.config(item.getKey, value)
    })

    val session = builder.getOrCreate()

    session.sparkContext.setLogLevel("WARN")

    session
  }
}
