package tgfdDiscovery

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{array, col, collect_list, lit, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import tgfdDiscovery.dependencyGeneration.DependencyGenerator
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

    val hdfsPath = "hdfs://data/imdb/completeGraph/new_imdb/"
    val localPath = "/Users/roy/Desktop/TGFD/datasets/imdb/300k_imdb/"
    val graphs = IMDBLoader.loadGraphSnapshots(spark, localPath, "170909", "171009")

    val firstGraph = graphs.head

    // 1. 统计最频繁的edges和vertex和attribute
    System.out.println("Vertex Types:")
    val vertexTypes = Histogram.countVertexTypes(firstGraph).map(_._1)
    vertexTypes.foreach(println)

    // 统计顶点属性
    System.out.println("Vertex Attributes:")
    val vertexAttributes = Histogram.countVertexAttributes(firstGraph)
    vertexAttributes.foreach(println)

    // 统计属性对应的顶点类型
    val vertexTypeToAttributes = Histogram.vertexTypeToAttributes(firstGraph)
    vertexTypeToAttributes.collect().foreach { case (vertexTypes, attribute) =>
      println(s"Vertex Type: $vertexTypes, Attributes: ${attribute.mkString(", ")}")
    }

    // 统计自定义边类型
    val customEdgeTypesCount = Histogram.countUniqueCustomEdgeTypes(firstGraph)
    customEdgeTypesCount.foreach { case (key, count) =>
      println(s"$key: $count")
    }
    val edgeTypes = customEdgeTypesCount.map(_._1)

    // 2. 基于统计结果生成新的pattern，这里的k是指最多的边数
    val vertexToAttribute: Map[String, List[String]] = Map(
      "actor" -> List("name"),
      "movie" -> List("year_of", "language_of", "name"),
      "actress" -> List("name"),
      "director" -> List("name"),
      "country" -> List("name"),
      "genre" -> List("name")
    )

    val patternsTree = PatternGenerator.generatePatternTrees(vertexTypes, edgeTypes, 5)

    val mergeFlagsUDF: UserDefinedFunction = udf((arrays: Seq[Seq[Int]]) => {
      arrays.transpose.map(_.reduce((a, b) => a | b))
    })

    patternsTree.levels.drop(2).foreach { level =>
      level.foreach { pattern =>
        val vertices = pattern.vertices
        val subMap = vertexToAttribute.filterKeys(vertices.contains)
        val dependencies = DependencyGenerator.generateCombinations(subMap)

        val frames: Seq[(DataFrame, Int)] = graphs.zipWithIndex.map { case (graph, index) =>
          val frame = PatternMatch.findMatches(spark, graph, pattern.edges)
          (frame, index)
        }

        dependencies.foreach { dependency =>
          val modifiedFrames = frames.map { case (frame, graphIndex) =>
            val modifiedFrame = PatternMatch.applyDependencyAttributes(frame, Seq(dependency))

            val flagArray = Array.fill(graphs.length)(0)
            flagArray(graphIndex) = 1

            modifiedFrame.withColumn("presence_flags", array(flagArray.map(lit): _*))
          }

          val combinedDf = modifiedFrames.reduce(_ unionByName _)

          val groupingColumns = combinedDf.columns.filterNot(_ == "presence_flags").map(col)

          val aggregatedDf = combinedDf
            .groupBy(groupingColumns: _*)
            .agg(
              collect_list("presence_flags").as("collected_flags")
            )
            .withColumn("presence_flags", mergeFlagsUDF(col("collected_flags")))
            .drop("collected_flags")

          aggregatedDf.show(false)
        }
      }
    }

    spark.stop()
  }

}
