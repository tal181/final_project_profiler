package com.project.checks.deequ.calculator

import com.amazon.deequ.analyzers.runners.{AnalysisRunner, AnalyzerContext, EmptyStateException}
import com.amazon.deequ.analyzers.{Analyzer, Size}
import com.amazon.deequ.custom.custom
import com.amazon.deequ.metrics.{DoubleMetric, Metric => DeequMetric}
import MetricsCalculator.convertToString
import com.project.checks.domain.{Metric, MetricResult, MetricWithParsedQuery, Table}
import com.project.checks.utils.ExceptionMessageConverter
import agg.{Count, CountDistinct}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedFunction}
import org.apache.spark.sql.catalyst.parser.ParserInterface

import scala.util.{Failure, Success}

/**
  * We try to decrease the number of times data is read from the remote storage. We do so by leveraging the Deequ
  * analyzers ability to group metrics and run them in a single query.
  * Not every metric can be translated to the Deequ language, if that is the case we propagate it to the next calculator
  * @param sqlParser parse column expression to the understandable by Spark format
  * @param metricsQueryParser translate default and optional metrics to the custom metric format
  * @param next the next calculator, acts as a fallback when a metric can not be run with Deequ
  */
case class DeequMetricsCalculator(sqlParser: ParserInterface,
                                  metricsQueryParser: MetricsQueryParser,
                                  maxMetricsPerQuery: Int,
                                  override protected val next: Option[MetricsCalculator] = None)
  extends MetricsCalculator {

  type DeequAnalyzer = Analyzer[_, DeequMetric[_]]

  def calculate(table: Table, metrics: Seq[Metric]): Seq[MetricResult] = {
    logger.debug(s"[Start] Calculating ${metrics.size} metrics")

    //first translate default and optional metrics to the custom metric format
    val (nonParsedMetrics, parsedMetrics) = metricsQueryParser.parse(metrics)

    //then we check if we know how to create an analyzer for the query
    //impl note -- at this stage this always result in the empty collection, but this might not be the case
    //when we start to support custom metrics
    val nonTranslatableToDeequMetrics = parsedMetrics
      .filterNot(metric => isAnalyzerDefinedForQuery(metric.parsedQuery))
      .map(_.metric)

    val analyzers: Map[MetricWithParsedQuery, Analyzer[_, DeequMetric[_]]] = parsedMetrics
      .filter(metric => isAnalyzerDefinedForQuery(metric.parsedQuery))
      .map(metric => (metric, metric.parsedQuery))
      .toMap
      .mapValues(createAnalyzerForQuery)

    log(nonParsedMetrics, nonTranslatableToDeequMetrics, analyzers)

    try {
      val updateJobDescription = jobDescriptionSetter(table, metrics.size)
      //multiple metrics may be calculated by one analyzers
      val results = eliminateDuplicates(analyzers)
        .grouped(maxMetricsPerQuery)
        .zipWithIndex
        .flatMap {
          case (analyzersGroup, iterationNumber) =>
            updateJobDescription(analyzersGroup.size, iterationNumber)

            val analysisResult = AnalysisRunner
              .onData(table.datum)
              .addAnalyzers(analyzersGroup)
              .run()

            toMetricsResults(invert(analyzers), analysisResult)
        }
        .toSeq

      val (succeededMetrics, failedMetrics) = partitionByResultType(results)
      log(failedMetrics)

//      every metric that wasn't parsed/translated or failed we propagate to the next calculator
      succeededMetrics ++ runNext(table,
                                  nonParsedMetrics ++ nonTranslatableToDeequMetrics ++ failedMetrics.map(_.metric))
    } catch {
      case ex: Throwable => onRuntimeErrorPropagateAllMetricsToNext(table, metrics)(ex)
    }
  }

  private def handleAnalysisResult(analyze: AnalyzerContext => Seq[MetricResult])(
    analysisResult: AnalyzerContext): Seq[MetricResult] = {
    analyze(analysisResult)
  }

  private def onRuntimeErrorPropagateAllMetricsToNext(table: Table, metrics: Seq[Metric])(
    ex: Throwable): Seq[MetricResult] = {
    logger.warn(
      """
        |Unexpected runtime failure while running metrics on Deequ.
        |This must not happen under normal circumstances.
        |Propagating all the metrics to the next calculator
        |""".stripMargin,
      ex
    )
    runNext(table, metrics)
  }

  private def partitionByResultType(result: Seq[MetricResult]): (Seq[MetricResult], Seq[MetricResult]) = {
    val succeededMetrics = result.collect { case succeeded @ MetricResult(_, Success(_)) => succeeded }
    val failedMetrics = result.collect { case failed @ MetricResult(_, Failure(_))       => failed }
    (succeededMetrics, failedMetrics)
  }

  //TODO: too many responsibilities -- it would be better to have the below code as an implicit conversion
  //TODO: that will be based on the metric type (e.g HLLMetric, DoubleMetric)
  private def toMetricsResults(metricsByAnalyzer: Map[Analyzer[_, _], Seq[MetricWithParsedQuery]],
                               analysisResult: AnalyzerContext): Seq[MetricResult] = {
    def toDoubleMetric(metric: DeequMetric[_]): DoubleMetric = metric.flatten().head
    def isCountLikeMetric(query: ParsedQuery): Boolean =
      query.aggFunction == Count || query.aggFunction == CountDistinct

    analysisResult.metricMap.flatMap {
      case (analyzer, metricResult) =>
        val metrics = metricsByAnalyzer(analyzer)

        //multiple metrics may be calculated by one analyzers
        metrics.map {
          case MetricWithParsedQuery(metric, parsedQuery) =>
            val finalResult = toDoubleMetric(metricResult).value.map(convertToString).recover {
              //for count-like query should return 0 when a metric returns no results
              case _: EmptyStateException if isCountLikeMetric(parsedQuery) => "0"
              //it will be handled later by the special cases calculator (a matter to change)
              case _: EmptyStateException => null
            }

            logger.debug(
              s"The result value for the metric " +
                s"(name: ${metric.metricName}, column: ${metric.column}, query: ${metric.query.value}) is $finalResult")
            MetricResult(metric, finalResult)
        }
    }
  }.toSeq

  private val createAnalyzerForQuery: PartialFunction[ParsedQuery, Analyzer[_, DeequMetric[_]]] = {
    case ParsedQuery(agg.Count, "*", condition)        => Size(condition)
    case ParsedQuery(agg.Count, expression, condition) => custom.Count(parse(expression), condition)
    case ParsedQuery(agg.CountDistinct, expression, None) if countDistinct.isSupportedByDeequ(expression) =>
      custom.CountDistinct(countDistinct.extractColumns(expression))
    case ParsedQuery(agg.Min, expression, condition)    => custom.Minimum(parse(expression), condition)
    case ParsedQuery(agg.Max, expression, condition)    => custom.Maximum(parse(expression), condition)
    case ParsedQuery(agg.StdDev, expression, condition) => custom.StandardDeviation(parse(expression), condition)
    case ParsedQuery(agg.Sum, expression, condition)    => custom.Sum(parse(expression), condition)
    case ParsedQuery(agg.Avg, expression, condition)    => custom.Mean(parse(expression), condition)
    case ParsedQuery(agg.Distinctness, expression, condition)    => custom.Distinctness(expression.split(","), condition)

  }

  private val isAnalyzerDefinedForQuery: Function[ParsedQuery, Boolean] =
    query => createAnalyzerForQuery.isDefinedAt(query)

  /**
    * Any column can be an expression, but here is the tricks.
    * Although Spark can correctly parse SQL statements when they are part of a SQL query, like in the following example:
    *
    *   `select MIN(LENGTH(col1)) FROM test`
    *
    * The same doesn't hold when we use the Dataset's API, e.g:
    *
    *   dataset.select("min(length(col1))")) or dataset.select(min("length(col1)")))
    *
    * In the above example we pass an expression as a plain string to the `select` method,
    * but this is illegal and yields an analysis exception:
    *
    *   column "min(length(col1))" is not found
    *
    * We must explicitly specify the column and the functions to make it work, e.g:
    *
    *    imports spark.sql.function._
    *    dataset.select(min(length(col("col1"))))
    *
    * To overcome this limitation we manually parse the expression with the Spark's sql parser, that generates
    * a valid AST that can be later used in the Dataset API, e.g:
    *
    *    dataset.select(parse("min(length(col1))")))
    *
    * Works properly
    */
  private def parse(expression: String): Column = new Column(sqlParser.parseExpression(expression))

  private def log(failedMetrics: Seq[MetricResult]): Unit = {
    if (failedMetrics.nonEmpty) {
      logger.debug(
        s"The number of failed metrics that will be propagated to the next calculator is ${failedMetrics.size}")

      val converter = ExceptionMessageConverter()
      val byExceptionType = failedMetrics.groupBy(_.value.recover { case e => converter.convert(e) })
      logger.debug(s"Grouped by a fail reason. The number of unique failures is: ${byExceptionType.keys.size}")
      byExceptionType.foreach {
        case (_, metrics) =>
          logger.debug(s"""
                         |${metrics.head.value}
                         |
                         |# of failed metrics with the above reason: ${metrics.size}
                         |""".stripMargin)
      }
    }
  }

  private def log(nonParsedMetrics: Seq[Metric],
                  nonTranslatableToDeequMetrics: Iterable[Metric],
                  analyzers: Map[MetricWithParsedQuery, Analyzer[_, DeequMetric[_]]]): Unit = {
    logger.debug(s"The number of metrics are going to be calculated on Deequ is ${analyzers.size}")
    logger.debug(s"The number of parsed metrics that can not be run on Deequ is ${nonTranslatableToDeequMetrics.size}")

    if (nonTranslatableToDeequMetrics.nonEmpty)
      logger.debug(
        s"Metrics that can not be run on Deequ, " +
          s"due to to the missing query->analyzer translation are: $nonTranslatableToDeequMetrics")

    logger.debug(
      s"The number of metrics are going to be calculated with the next calculator is " +
        s"${nonParsedMetrics.size + nonTranslatableToDeequMetrics.size}")
  }

  //TODO: too many responsibilities -- it would be better to have the below code in some Deequ utility class
  // since it doesn't need any state to operate
  private def eliminateDuplicates(analyzers: Map[MetricWithParsedQuery, DeequAnalyzer]): Seq[DeequAnalyzer] = {
    val allAnalyzers = analyzers.values.toSeq
    val distinctAnalyzers = allAnalyzers.distinct

    if (distinctAnalyzers.size == allAnalyzers.size) {
      //no duplicate analyzers found, we are good to go
      return analyzers.values.toSeq
    }

    val metricsByAnalyzer: Map[DeequAnalyzer, Seq[MetricWithParsedQuery]] = invert(analyzers)
    val duplicateAnalyzers = allAnalyzers.diff(distinctAnalyzers).distinct

    logDuplicates(metricsByAnalyzer, duplicateAnalyzers)

    distinctAnalyzers
  }

  private def logDuplicates(metricsByAnalyzer: Map[DeequAnalyzer, Seq[MetricWithParsedQuery]],
                            duplicateAnalyzers: Seq[DeequAnalyzer]): Unit = {
    duplicateAnalyzers.foreach(duplicateAnalyzer => {
      logger.debug(
        s"Duplicate analyzer found: $duplicateAnalyzer. " +
          s"It was created by the following metrics: ${metricsByAnalyzer(duplicateAnalyzer).map(_.metric)}")
    })
  }

  private object countDistinct {
    //count distinct requires a special treatment because the way it is calculated and stored by Deequ
    //it accepts only a list of column names, functions or nested columns are not allowed
    //Examples of a valid expression:
    //    count(distinct col1)
    //    count(distinct col1, col2)
    //Example of invalid expression
    //    count(distinct length(col1), col2)
    private[calculator] def isSupportedByDeequ(expression: String): Boolean = {
      logger.debug(s"Checking if expression [ $expression ] is supported by the Deequ analyzers")
      val ast = parse(s"count(distinct $expression)")
      //check expression is not a function or a nested column
      val result = ast.expr match {
        //count(arguments)
        case UnresolvedFunction(_, arguments, _, _, _)
            //all arguments of a function must be attributes!
            if arguments.forall(arg =>
              arg.isInstanceOf[UnresolvedAttribute]
                && arg.asInstanceOf[UnresolvedAttribute].nameParts.size == 1) =>
          true
        case _ => false
      }
      logger.debug(s"Check result is $result")
      result
    }

    private[calculator] def extractColumns(expression: String): Seq[String] = {
      val ast = parse(s"count(distinct $expression)")
      ast.expr
        .asInstanceOf[UnresolvedFunction]
        .arguments
        .asInstanceOf[Seq[UnresolvedAttribute]]
        .map(_.name)
    }
  }

  private def jobDescriptionSetter(table: Table, totalMetrics: Int): (Int, Int) => Unit = {
    (parsedMetrics: Int, iterationNumber: Int) =>
      setJobDescription(table, parsedMetrics, totalMetrics, maxMetricsPerQuery * iterationNumber)
  }

  private def setJobDescription(table: Table, parsedMetrics: Int, totalMetrics: Int, calculatedMetrics: Int): Unit = {
    table.datum.sparkSession.sparkContext
      .setJobDescription(s"Calculating $parsedMetrics out of ${totalMetrics - calculatedMetrics} metrics")
  }
}
