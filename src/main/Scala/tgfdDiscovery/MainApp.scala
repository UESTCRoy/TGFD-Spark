package tgfdDiscovery

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.histogram.Histogram
import tgfdDiscovery.loader.IMDBLoader
import tgfdDiscovery.patternGeneration.PatternGeneration

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
    val graph = IMDBLoader.loadGraphSnapshots(spark, localPath, "170909", "171009")

    // 1. 统计最频繁的edges和vertex和attribute
    val firstGraph = graph.head

    System.out.println("Vertex Types:")
    val vertexTypes = Histogram.countVertexTypes(firstGraph).map(_._1)
    vertexTypes.foreach(println)

    // 统计边类型
//    System.out.println("Edge Types:")
//    val edgeTypes = Histogram.countEdgeTypes(firstGraph)
//    edgeTypes.foreach(println)

    // 统计顶点属性
    System.out.println("Vertex Attributes:")
    val vertexAttributes = Histogram.countVertexAttributes(firstGraph)
    vertexAttributes.foreach(println)

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

//
//    // 3. 使用生成的pattern从graph里找matches
//    val matches = PatternMatch.run(spark, graph, patterns)
//
//    // 处理和输出匹配结果
//    outputMatches(matches)

    // 结束Spark会话
    spark.stop()
  }

}
