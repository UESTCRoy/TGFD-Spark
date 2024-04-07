package tgfdDiscovery

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.dependencyGenerator.DependencyGenerator
import tgfdDiscovery.histogram.Histogram
import tgfdDiscovery.loader.IMDBLoader
import tgfdDiscovery.patternGenerator.PatternGenerator
import tgfdDiscovery.patternMatch.PatternMatch

object MainApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TGFD Discovery")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val localPath = "/Users/roy/Desktop/TGFD/datasets/imdb/300k_imdb/"
    val graphs = IMDBLoader.loadGraphSnapshots(spark, localPath, "170909", "171009")

    val firstGraph = graphs.head

    // 统计和生成模式
    val vertexTypes = Histogram.countVertexTypes(firstGraph).map(_._1)
    val edgeTypes = Histogram.countUniqueCustomEdgeTypes(firstGraph).map(_._1)
    val vertexToAttribute: Map[String, List[String]] = Map(
      "actor" -> List("name"),
      "movie" -> List("year_of", "language_of", "name"),
      "actress" -> List("name"),
      "director" -> List("name"),
      "country" -> List("name"),
      "genre" -> List("name")
    )
    val patternsTree = PatternGenerator.generatePatternTrees(vertexTypes, edgeTypes, 5)

    patternsTree.levels.drop(2).foreach { level =>
      level.foreach { pattern =>
        val framesWithIndex = graphs.zipWithIndex.map { case (graph, index) =>
          val frame = PatternMatch.findMatches(spark, graph, pattern.edges)
          (frame, index)
        }
        val dependencies = DependencyGenerator.generateCandidateDependency(pattern.vertices, vertexToAttribute)

        dependencies.foreach {dependency => {
          val combinedDf = PatternMatch.processFramesAndDependency(framesWithIndex, dependency)
          combinedDf.show(false)
          // TODO: Deal with Negative TGFD by groupBy LHS. Transform Dataframe into DataSets
          // TGFD: Pattern, Dependency, Delta

        }}
      }
    }

    spark.stop()
  }
}
