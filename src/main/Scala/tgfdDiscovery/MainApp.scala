package tgfdDiscovery

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.histogram.Histogram
import tgfdDiscovery.loader.IMDBLoader
import tgfdDiscovery.patternGeneration.PatternGeneration
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

    // 统计边类型
//    System.out.println("Edge Types:")
//    val edgeTypes = Histogram.countEdgeTypes(firstGraph)
//    edgeTypes.foreach(println)

    // 统计顶点属性
//    System.out.println("Vertex Attributes:")
//    val vertexAttributes = Histogram.countVertexAttributes(firstGraph)
//    vertexAttributes.foreach(println)

    // 统计属性对应的顶点类型
    val attributeVertexTypeMap = Histogram.attributeToVertexTypes(firstGraph)
    attributeVertexTypeMap.collect().foreach { case (attribute, vertexTypes) =>
      println(s"Attribute: $attribute, Vertex Types: ${vertexTypes.mkString(", ")}")
    }

    // 统计自定义边类型
    val customEdgeTypesCount = Histogram.countUniqueCustomEdgeTypes(firstGraph)
    customEdgeTypesCount.foreach { case (key, count) =>
      println(s"$key: $count")
    }
    val edgeTypes = customEdgeTypesCount.map(_._1)

    // 2. 基于统计结果生成新的pattern
    val patternsTree = PatternGeneration.generatePatternTrees(vertexTypes, edgeTypes, 5)

    // 3. 使用生成的pattern从graph里找matches
//    PatternMatch.findMatches(spark, firstGraph)

    // TODO: 修改为关于VertexType与attribute的映射关系
    val attributesMapping = Map(
      "actor.name" -> "actor",
      "movie.year_of" -> "movie",
      "movie.language_of" -> "movie",
      "movie.name" -> "movie",
      "actress.name" -> "actress"
    )

    import scala.collection.mutable

    // 初始化用于存储所有可能依赖关系的集合
    val dependencies = mutable.Set.empty[(Set[String], String)]

    // 给定的顶点类型集合
//    val patternVertexTypes = Set("actor", "movie", "actress", "director")
    val patternVertexTypes = Set("actor", "movie", "actress")

    // 计算左侧属性的数量
    val leftSideCount = patternVertexTypes.size - 1

    // 生成所有属性的组合，确保组合中的属性来自不同的顶点类型
    val allCombinations = attributesMapping.keys.toSet.subsets(leftSideCount).filter { subset =>
      subset.map(attributesMapping).size == leftSideCount
    }

    // 对于每个可能的左侧组合，找到可能的右侧属性
    allCombinations.foreach { combination =>
      val leftTypes = combination.map(attributesMapping)
      val possibleRightTypes = patternVertexTypes.diff(leftTypes)

      possibleRightTypes.foreach { rightType =>
        attributesMapping.filter { case (attr, vType) => vType == rightType }.keys.foreach { rightAttr =>
          dependencies.add((combination, rightAttr))
        }
      }
    }

    // 打印生成的依赖关系
    dependencies.foreach { case (inputs, result) =>
      println(s"(${inputs.mkString(", ")}) → $result")
    }



    // 结束Spark会话
    spark.stop()
  }

}
