package com.project.checks.repository.writer.impl

import com.project.checks.domain.{CheckResult, DeequCheckConfig, MetricResult}
import com.project.checks.repository.writer.StorageHandler
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success}

/**
 * responsible for storing the results of the process
 *
 * @param config The app configuration
 * @param client The metrics results client
 */
class StorageHandlerImpl(outputPath: String, spark: SparkSession)
  extends StorageHandler
    with Serializable {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def storeChecks(results: Seq[CheckResult]): Unit = {
    if (results.isEmpty) {
      logger.warn(s"Didn't store metrics since there were no results")
      //throw DataQualityClientException(message = "Didn't store metrics since there were no results")
    }

    try {
      storeChecksInternal(results)
    } catch {
      case e: Exception =>
        val msg = e.getMessage
        logger.error(msg)
        throw e
    }
  }

  override def storeMetrics(results: Seq[MetricResult]): Unit = {
    if (results.isEmpty) {
      logger.warn(s"Didn't store metrics since there were no results")
      //throw DataQualityClientException(message = "Didn't store metrics since there were no results")
    }

    try {
      doStore(results)
    } catch {
      case e: Exception =>
        val msg = e.getMessage
        logger.error(msg)
        throw e
    }
  }

  def storeChecksInternal(results: Seq[CheckResult]): Unit = {
    logger.debug(s"Sending to output" + outputPath)
    import spark.implicits._
    val columns = Seq("checkName", "status")
    val res = results.map(res => (res.checkName, res.status))
    val df = spark.createDataFrame(res).toDF(columns: _*)
    df.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputPath)
  }


  def doStore(results: Seq[MetricResult]): Unit = {
    logger.debug(s"Sending to output" + outputPath)
    import spark.implicits._
    val columns = Seq("name", "column", "value")
    val res = results.map(res => (res.metric.metricName, res.metric.column, res.value.get))
    val df = spark.createDataFrame(res).toDF(columns: _*)
    df.coalesce(1).write.option("header", "true").mode(SaveMode.Overwrite).csv(outputPath)
  }

}

object StorageHandlerImpl {

  def apply(outputPath: String, spark: SparkSession): StorageHandlerImpl = {
    new StorageHandlerImpl(outputPath, spark)
  }
}
