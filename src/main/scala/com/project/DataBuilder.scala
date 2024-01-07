package com.project

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.utils.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object DataBuilder {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val cli = new CommandLineArgs(args)

    val spark = configureSpark()
    // Load the data into 2 dataframes
    val df1 = spark.read.option("header", "true").csv("/Users/tsharon/IdeaProjects/final_project/src/main/resources/medications.csv")


    val df2 = df1.union(df1).union(df1).union(df1).union(df1)

    logger.info("before")
    df2.coalesce(1).write.option("header", "true").csv("test_1.csv")
    logger.info("after")

  }

  private def configureSpark(): SparkSession = {
    SparkSessionFactory.createSession("spark-local.properties")
  }
}

