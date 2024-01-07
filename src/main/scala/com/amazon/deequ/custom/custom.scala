package com.amazon.deequ.custom

import com.amazon.deequ.analyzers.Analyzers.{COUNT_COL, conditionalSelection, ifNoNullsIn}
import com.amazon.deequ.analyzers.{FilterableAnalyzer, MaxState, MeanState, MinState, NumMatches, ScanShareableFrequencyBasedAnalyzer, StandardDeviationState, StandardScanShareableAnalyzer, SumState}
import com.amazon.deequ.metrics.{DoubleMetric, Entity}
import org.apache.spark.sql.DeequFunctions.stateful_stddev_pop
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row}

/**
  * A set of custom Deequ analyzers
  * The main differences from the built-in analyzers are:
  *  1. column can be an expression
  *  2. preconditions are relaxed
  */
object custom {

  case class Minimum(column: Column, where: Option[String] = None)
    extends StandardScanShareableAnalyzer[MinState]("Minimum", column.expr.prettyName)
    with FilterableAnalyzer {

    override def aggregationFunctions(): Seq[Column] = {
      min(conditionalSelection(column, where)).cast(DoubleType) :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int): Option[MinState] = {
      ifNoNullsIn(result, offset) { _ =>
        MinState(result.getDouble(offset))
      }
    }

    protected override def additionalPreconditions(): Seq[StructType => Unit] = {
      Nil
    }

    override def filterCondition: Option[String] = where
  }

  case class Maximum(column: Column, where: Option[String] = None)
    extends StandardScanShareableAnalyzer[MaxState]("Maximum", column.expr.prettyName)
    with FilterableAnalyzer {

    override def aggregationFunctions(): Seq[Column] = {
      max(conditionalSelection(column, where)).cast(DoubleType) :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int): Option[MaxState] = {
      ifNoNullsIn(result, offset) { _ =>
        MaxState(result.getDouble(offset))
      }
    }

    override protected def additionalPreconditions(): Seq[StructType => Unit] = {
      Nil
    }

    override def filterCondition: Option[String] = where
  }

  case class StandardDeviation(column: Column, where: Option[String] = None)
    extends StandardScanShareableAnalyzer[StandardDeviationState]("StandardDeviation", column.expr.prettyName)
    with FilterableAnalyzer {

    override def aggregationFunctions(): Seq[Column] = {
      stateful_stddev_pop(conditionalSelection(column, where)) :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int): Option[StandardDeviationState] = {
      if (result.isNullAt(offset)) {
        None
      } else {
        val row = result.getAs[Row](offset)
        val n = row.getDouble(0)

        if (n == 0.0) {
          None
        } else {
          Some(StandardDeviationState(n, row.getDouble(1), row.getDouble(2)))
        }
      }
    }

    override protected def additionalPreconditions(): Seq[StructType => Unit] = {
      Nil
    }

    override def filterCondition: Option[String] = where
  }

  case class Sum(column: Column, where: Option[String] = None)
    extends StandardScanShareableAnalyzer[SumState]("Sum", column.expr.prettyName)
    with FilterableAnalyzer {

    override def aggregationFunctions(): Seq[Column] = {
      sum(conditionalSelection(column, where)).cast(DoubleType) :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int): Option[SumState] = {
      ifNoNullsIn(result, offset) { _ =>
        SumState(result.getDouble(offset))
      }
    }

    override protected def additionalPreconditions(): Seq[StructType => Unit] = {
      Nil
    }

    override def filterCondition: Option[String] = where
  }

  case class Mean(column: Column, where: Option[String] = None)
    extends StandardScanShareableAnalyzer[MeanState]("Mean", column.expr.prettyName)
    with FilterableAnalyzer {

    override def aggregationFunctions(): Seq[Column] = {
      sum(conditionalSelection(column, where)).cast(DoubleType) ::
        count(conditionalSelection(column, where)).cast(LongType) :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int): Option[MeanState] = {
      ifNoNullsIn(result, offset, howMany = 2) { _ =>
        MeanState(result.getDouble(offset), result.getLong(offset + 1))
      }
    }

    override protected def additionalPreconditions(): Seq[StructType => Unit] = {
      Nil
    }

    override def filterCondition: Option[String] = where
  }

  //accepts nested types
  case class CountDistinct(columns: Seq[String]) extends ScanShareableFrequencyBasedAnalyzer("CountDistinct", columns) {

    override def aggregationFunctions(numRows: Long): Seq[Column] = {
      count("*") :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int, fullColumn: Option[Column] = None): DoubleMetric = {
      toSuccessMetric(result.getLong(offset).toDouble)
    }

    override def preconditions: Seq[StructType => Unit] = Nil
  }

  object CountDistinct {
    def apply(column: String): CountDistinct = {
      new CountDistinct(column :: Nil)
    }
  }

  case class Count(column: Column, where: Option[String] = None)
    extends StandardScanShareableAnalyzer[NumMatches]("Count", column.expr.prettyName, Entity.Column)
    with FilterableAnalyzer {

    override def aggregationFunctions(): Seq[Column] = {
      count(conditionalSelection(column, where)).cast(LongType) :: Nil
    }

    override def fromAggregationResult(result: Row, offset: Int): Option[NumMatches] = {
      ifNoNullsIn(result, offset) { _ =>
        NumMatches(result.getLong(offset))
      }
    }

    override def filterCondition: Option[String] = where
  }

  case class Distinctness(columns: Seq[String], where: Option[String] = None)
    extends ScanShareableFrequencyBasedAnalyzer("Distinctness", columns)
      with FilterableAnalyzer {

    override def aggregationFunctions(numRows: Long): Seq[Column] = {
      (sum(col(COUNT_COL).geq(1).cast(DoubleType))) :: Nil
    }

    override def filterCondition: Option[String] = where
  }

  object Distinctness {
    def apply(column: String): Distinctness = {
      new Distinctness(column :: Nil)
    }
  }
}
