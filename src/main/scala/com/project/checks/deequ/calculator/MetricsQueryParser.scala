package com.project.checks.deequ.calculator

import com.amazon.deequ.analyzers.Distinctness
import MetricsQueryParser._
import com.project.checks.domain
import com.project.checks.domain.{Metric, MetricWithParsedQuery, QueryAst}
import com.project.checks.sql.agg
import com.project.checks.sql.agg.AggFunction
import com.project.checks.utils.Utils.extractors.DefinitionNameEq
import org.slf4j.{Logger, LoggerFactory}

/**
  * Acts as an adapter layer
  * Parse Default and Optional metrics definitions to the custom metrics format
  */
case class MetricsQueryParser() {
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  type ParserResult = (Seq[Metric], Seq[MetricWithParsedQuery])

  /**
    * Custom metrics carry additional information in terms of query AST which we can leverage
    * to parse the query
    * @param metrics the list of metrics
    * @return a pair where
    *         the left variable stands for the metrics we don't know how to parse
    *         the right variable returns the map where each metrics points to the parsed query
    */
  def parse(metrics: Seq[Metric]): ParserResult = {
    logger.debug(s"[Start] Parsing ${metrics.size} metrics")

    val groupedByParserType = metrics.groupBy(parserType)

    val parsedByDefinitionMetrics =
      groupedByParserType
        .getOrElse(DefinitionNameParserGroup, Seq.empty)
        .map(metric => metric -> metric)
        .toMap
        .mapValues(parseByDefinitionName)

    logger.debug(s"The number of metrics parsed by the definition id is ${parsedByDefinitionMetrics.size}")

    val (nonParsedMetrics, parsedByQueryAstMetrics) =
      groupedByParserType
        .getOrElse(QueryAstParserGroup, Seq.empty)
//        .map(metric -> parseByQueryAST(metric))
        .map(metric => metric -> metric)
        .toMap
        .mapValues(parseByQueryAST)
        .partition { case (_, parsedQuery) => parsedQuery.isEmpty }

    log(nonParsedMetrics, parsedByQueryAstMetrics)

    val parsedMetrics = (parsedByDefinitionMetrics ++ parsedByQueryAstMetrics.mapValues(_.get)).map {
      case (metric, parsedQuery) => domain.MetricWithParsedQuery(metric, parsedQuery)
    }.toSeq

    logger.debug(s"[End] Parsing metrics")
    (nonParsedMetrics.keys.toSeq, parsedMetrics)
  }

  private def parseByQueryAST(metric: Metric): Option[ParsedQuery] = metric.query.ast match {
    case Some(ast) =>
      logger.debug(s"Trying to parse metric (query: ${metric.query} based on the query ast - $ast")
      val result = doParseAst(ast)
      logger.debug(s"Parsing result -- $result (None means was unable to parse)")
      if (result.isEmpty) logger.debug(s"Metric (name: ${metric.metricName}) could not be parsed. AST - $ast ")
      result
    case None => None
  }

  private def doParseAst(queryAst: QueryAst): Option[ParsedQuery] = {
    queryAst.aggFunction.toLowerCase match {
      case "count"         => Option(ParsedQuery(agg.Count, queryAst.columnExpr, queryAst.condition))
      case "countdistinct" => Option(ParsedQuery(agg.CountDistinct, queryAst.columnExpr, queryAst.condition))
      case "max"           => Option(ParsedQuery(agg.Max, queryAst.columnExpr, queryAst.condition))
      case "min"           => Option(ParsedQuery(agg.Min, queryAst.columnExpr, queryAst.condition))

      case _               => None
    }
  }

  private val parserType: Metric => String =
    metric => if (parseByDefinitionName.isDefinedAt(metric)) DefinitionNameParserGroup else QueryAstParserGroup

  private val parseByDefinitionName: PartialFunction[Metric, ParsedQuery] = {
    //SELECT COUNT(*) as metricName FROM {table_name} WHERE {column} = "0"
    case DefinitionNameEq(CountZeros, column) => ParsedQuery(agg.Count, "*", Option(s"$column = 0 or $column = 0.00"))
    //SELECT COUNT(*) as metricName FROM {table_name}
    case DefinitionNameEq(CountRows, _) => ParsedQuery(agg.Count, "*")
    //SELECT COUNT(*) as metricName FROM {table_name} WHERE {query_param} IS NULL
    case DefinitionNameEq(CountNulls, column) => ParsedQuery(agg.Count, "*", Option(s"$column IS NULL"))
    //SELECT COUNT(*) as metricName FROM {table_name} WHERE {query_param} = ""
    case DefinitionNameEq(CountEmpties, column) =>
      ParsedQuery(agg.Count, "*", Option(s""" $column = "" """.trim))
    //SELECT COUNT(DISTINCT({query_param})) as metricName FROM {table_name}
    case DefinitionNameEq(CountUniques, column) =>
      ParsedQuery(agg.CountDistinct, column)

    case DefinitionNameEq(CountDuplicates, _) =>
      ParsedQuery(agg.CountDistinct, "*")

    //SELECT COUNT(*) as metricName FROM {table_name} WHERE BIGINT({query_param}) IS NOT NULL AND DOUBLE({query_param}) = BIGINT({query_param})
    case DefinitionNameEq(CountIntegers, column) =>
      ParsedQuery(agg.Count, "*", Option(s"BIGINT($column) IS NOT NULL AND DOUBLE($column) = BIGINT($column)"))
    //SELECT COALESCE( (SELECT MAX({query_param}) as metricName FROM {table_name}), 0) as metricName
    case DefinitionNameEq(Max, column) => {
      ParsedQuery(agg.Max, s"datediff($column, current_date())")
    }

    case DefinitionNameEq(CountMinNumeric, column) =>
      ParsedQuery(agg.Count, "*", Option(s"$column <= 0 or $column <= 0.00"))

    case DefinitionNameEq(Dis, column) => {
      ParsedQuery(agg.Distinctness, column)
    }
  }

  private def log(nonParsedMetrics: Map[Metric, Option[ParsedQuery]],
                  parsedByQueryAstMetrics: Map[Metric, Option[ParsedQuery]]): Unit = {
    logger.debug(s"The number of metrics parsed by the query ast is ${parsedByQueryAstMetrics.size}")
    logger.debug(s"The number of metrics that can not be parsed is  ${nonParsedMetrics.size}")

    logger.debug(
      s"The list of metrics that weren't parsed is: \n\t${nonParsedMetrics.keys.filter(_.query.ast.isEmpty)}")
    val nonParsedMetricsWithQueryAst = nonParsedMetrics.keys.filter(_.query.ast.nonEmpty)
    if (nonParsedMetricsWithQueryAst.nonEmpty)
      logger.debug(
        s"The list of metrics with query AST that weren't parsed is: \n\t${nonParsedMetricsWithQueryAst}")
  }
}

object MetricsQueryParser {
  val CountRows = "count rows"
  val CountNulls = "count nulls"
  val CountZeros = "count zeros"
  val CountEmpties = "count empties"
  val CountUniques = "count uniques"
  val CountIntegers = "count integers"
  val CountDuplicates = "count duplicates"
  val Max = "max"
  val CountMinNumeric = "count min numeric"
  val Dis = "Distinctness"

  private val DefinitionNameParserGroup = "definitionNameParserGroup"
  private val QueryAstParserGroup = "queryAstParserGroup"
}

case class ParsedQuery(aggFunction: AggFunction, columnExpression: String, condition: Option[String] = None)
