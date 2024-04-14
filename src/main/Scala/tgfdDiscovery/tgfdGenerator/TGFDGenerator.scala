package tgfdDiscovery.tgfdGenerator

import org.apache.spark.sql.functions.{col, collect_list, expr, first, udf}
import org.apache.spark.sql.DataFrame
import tgfdDiscovery.common.Pattern
import tgfdDiscovery.dependencyGenerator.DependencyGenerator
import tgfdDiscovery.supportCompute.SupportCompute
import tgfdDiscovery.common.TGFD
import tgfdDiscovery.deltaGenerator.DeltaGenerator

import scala.collection.mutable.ListBuffer

object TGFDGenerator {
  val flattenListUDF = udf((lists: Seq[Seq[Int]]) => lists.flatten)

  def processTGFDs(combinedDf: DataFrame, pattern: Pattern, dependencies: Set[String], dfCount: Long): List[TGFD] = {
    val results = ListBuffer[TGFD]()
    val lhsRhsCombinations = DependencyGenerator.generateLhsRhsCombinations(dependencies)

    lhsRhsCombinations.foreach { case (lhs, rhs) =>
      val lhsColumns = lhs.toSeq.map(col)
      val aggregatedDf = combinedDf
        .groupBy(lhsColumns: _*)
        .agg(
          first(col(rhs)).as(s"${rhs}"),
          expr(s"size(collect_list(`$rhs`))").as("rhsListSize"),
          collect_list("presence_flags").as("collected_flags")
        )
        .withColumn("collected_flags", flattenListUDF(col("collected_flags")))
        .filter(col("rhsListSize") <= 1)
        .drop("rhsListSize")

      val tgfdResults = aggregatedDf.collect().flatMap { row =>
        val flags = row.getAs[Seq[Int]]("collected_flags")
        DeltaGenerator.getMinMaxPair(flags.toList) match {
          case Some((minDelta, maxDelta)) =>
            val delta = (minDelta, maxDelta)
            val support = SupportCompute.calculateTGFDSupport(flags.toList, dfCount, flags.size)
            val lhsValues = lhs.map(l => row.getAs[String](l))
            val rhsValue = row.getAs[String](rhs)
            Some(TGFD(pattern, lhsValues.toSet, rhsValue, delta))
          case None => None
        }
      }

      results ++= tgfdResults
    }

    results.toList
  }
}
