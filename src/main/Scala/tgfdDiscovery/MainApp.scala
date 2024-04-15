package tgfdDiscovery

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tgfdDiscovery.dependencyGenerator.DependencyGenerator
import tgfdDiscovery.histogram.Histogram
import tgfdDiscovery.loader.IMDBLoader
import tgfdDiscovery.patternGenerator.PatternGenerator
import tgfdDiscovery.patternMatch.PatternMatch
import tgfdDiscovery.tgfdGenerator.TGFDGenerator

object MainApp {
  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TGFD Discovery")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val startTime = System.currentTimeMillis()

    val dataSet = args(0)
    val localPath = args(1)
    val k = args(2).toInt
    val startDate = args(3)
    val endDate = args(4)
    val tgfdTheta = args(5).toDouble

    //    val localPath = "/Users/roy/Desktop/TGFD/datasets/imdb/300k_imdb/"
    val graphs = IMDBLoader.loadGraphSnapshots(spark, localPath, startDate, endDate)
    logger.info("Loaded graph snapshots.")

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
    val patternsTree = PatternGenerator.generatePatternTrees(vertexTypes, edgeTypes, k)

    patternsTree.levels.drop(2).zipWithIndex.foreach { case (level, index) =>
      logger.info(s"\n\n--- Processing level ${index + 2} of the pattern tree ---")
      level.foreach { pattern =>
        val framesWithIndex = graphs.zipWithIndex.map { case (graph, graphIndex) =>
          val frame = PatternMatch.findMatches(spark, graph, pattern.edges)
          (frame, graphIndex)
        }
        val dependencies = DependencyGenerator.generateCandidateDependency(pattern.vertices, vertexToAttribute)

        dependencies.foreach { dependency =>
          val combinedDf = PatternMatch.processFramesAndDependency(framesWithIndex, dependency)
          val dfCount = combinedDf.count()

          val results = TGFDGenerator.processTGFDs(combinedDf, pattern, dependency, dfCount, tgfdTheta)
          logger.info(s"Processed ${results.length} TGFDs for dependency: $dependency")
        }
      }
    }

    val endTime = System.currentTimeMillis()
    logger.info(s"Application finished. Total execution time: ${(endTime - startTime) / 1000} seconds")
    spark.stop()
  }
}
