package tgfdDiscovery

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession}
import tgfdDiscovery.dependencyGeneration.DependencyGenerator
import tgfdDiscovery.histogram.Histogram
import tgfdDiscovery.loader.IMDBLoader
import tgfdDiscovery.patternGenerator.PatternGenerator
import tgfdDiscovery.patternMatch.PatternMatch

object MainApp {
  def main(args: Array[String]): Unit = {
    // 初始化SparkSession
    val spark = SparkSession.builder()
      .appName("TGFD Discovery")
      .master("local[*]") // 或者根据需要配置为其他模式
      .getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 读取IMDB数据
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
    /*
        1. 从k = 2开始，遍历每一层的pattern
        2. 每一层pattern的每一个pattern，进行findMatches
        3. 在这个pattern里生成dependency，然后对找到的Matches进行withColumn，然后groupByKey
     */
    // 先遍历pattern, 在遍历graph, 然后把找到的matches进行groupByKey的lhs
    patternsTree.levels.drop(2).foreach { level =>
      level.foreach { pattern =>
        // Generate dependencies
        val vertices = pattern.vertices
        val subMap = vertexToAttribute.filterKeys(vertices.contains)
        val dependencies = DependencyGenerator.generateCombinations(subMap)

        val frames: Seq[DataFrame] = graphs.map(graph => {
          // Find matches for each graph and collect the resulting DataFrames
          PatternMatch.findMatches(spark, graph, pattern.edges)
        })
//        val combinedFrame = frames.reduce(_ union _)

        dependencies.foreach { dependency =>
          val modifiedFrames = graphs.map { graph =>
            val matches = PatternMatch.findMatches(spark, graph, pattern.edges)
            val modifiedFrame = PatternMatch.applyDependencyAttributes(matches, Seq(dependency))
            modifiedFrame.show()
            modifiedFrame
          }

          // 如果需要，可以在这里处理modifiedFrames（例如，合并或进一步分析）
          // 示例：合并所有修改后的DataFrame为一个DataFrame
          val combinedFrame = if (modifiedFrames.nonEmpty) modifiedFrames.reduce(_ union _) else spark.emptyDataFrame
          // 进行更多操作，比如展示合并后的DataFrame
          combinedFrame.show()
        }

      }
    }

    // 3. 使用生成的pattern从graph里找matches
    //    PatternMatch.findMatches(spark, firstGraph)

    // 结束Spark会话
    spark.stop()
  }

}
