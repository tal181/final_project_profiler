package com.project

import com.project.checks.configuration.CommandLineArgs
import com.project.checks.utils.SparkSessionFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object VisualizationComponent {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val cli = new CommandLineArgs(args)

    val spark = configureSpark()
    // Load the data into 2 dataframes
    val df1 = spark.read.option("header", "true").csv(cli.dataSetOutputSqlPath)
      .withColumnRenamed("checkName", "sqlName")
      .withColumnRenamed("status", "sqlValue")
    val df2 = spark.read.option("header", "true").csv(cli.dataSetOutputDeequPath)
      .withColumnRenamed("checkName", "checkName")
      .withColumnRenamed("status", "DeequValue")
    val df3 = spark.read.option("header", "true").csv(cli.dataSetOutputDqDFPath)
      .withColumnRenamed("checkName", "DQDFName")
      .withColumnRenamed("status", "DQDFValue")

    // Next join the two dataframes using an INNER JOIN on the key as follows:
    val joined = df1.join(df2, df1.col("sqlName") === df2.col("checkName"), "outer")
      .join(df3, df2.col("checkName") === df3.col("DQDFName"), "outer")

    val res = joined.selectExpr("checkName", "sqlValue","DQDFValue", "DeequValue")

    logger.info("All checks")
    res.show(100, false)

    logger.info("Diff is")
    res
      .filter("(sqlValue <> DeequValue or DQDFValue <> DeequValue) or ( DeequValue is null and sqlValue is not null) or ( sqlValue is null and DeequValue is not null) or ( DQDFValue is null and DeequValue is not null)")
      .show(100, false)

    res.coalesce(1).write.csv("results.csv")
  }

  private def configureSpark(): SparkSession = {
    SparkSessionFactory.createSession("spark-local.properties")
  }
}

