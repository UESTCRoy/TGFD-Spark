package tgfdDiscovery.script

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import IMDBGraphUtils.{createIMDBEdges, createIMDBVertices, extractIMDBType, extractIMDBVertexURI, isDesiredType}
import tgfdDiscovery.common.VertexData
import tgfdDiscovery.script.LocalIMDBReader.countPredicates

import scala.util.matching.Regex

object CustomIMDBGraph {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val typeRegex: Regex = ".*/([^/]+)/[^/]*>$".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("Generate Custom IMDB Graph")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)
    val edgeSize = args(2)

    //    val spark = SparkSession
    //      .builder
    //      .appName("RDF Graph Loader")
    //      .master("local[*]")
    //      .getOrCreate()
    //
    //    val inputDir = "/Users/roy/Desktop/TGFD/datasets/imdb/1m_imdb_old/imdb-170909.nt"
    //    val outputDir = "/Users/roy/Desktop/TGFD/test"
    //    val edgeSize = 1000000

    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
    val fileStatusList = fs.listStatus(new Path(inputDir))

    var previousSelectedEdges: RDD[Edge[String]] = spark.sparkContext.emptyRDD[Edge[String]]

    fileStatusList.foreach { fileStatus =>
      val filePath = fileStatus.getPath.toString
      val fileName = fileStatus.getPath.getName
      val fileContent = spark.sparkContext.textFile(filePath)
      println(s"Processing file: $filePath")

      val desiredTypes = Set("movie", "actor", "director", "country", "actress", "genre")

      // 解析文件内容
      val parsedTriplets = fileContent.flatMap { line =>
        Option(line).map(_.trim.dropRight(1).replaceAll("""\^\^<http://www\.w3\.org/2001/XMLSchema#(float|integer)>""", "")).flatMap { trimmedLine =>
          val uriMatches = uriRegex.findAllIn(trimmedLine).toList
          val literalMatches = literalRegex.findAllIn(trimmedLine).toList

          (uriMatches, literalMatches) match {
            case (List(subject, predicate), List(literal)) if isDesiredType(subject, desiredTypes) =>
              val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
              Some((subject, attributeName, literal.replaceAll("\"", ""), "attribute"))
            case (List(subject, predicate, obj), _) if isDesiredType(subject, desiredTypes) && isDesiredType(obj, desiredTypes) =>
              val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
              Some((subject, attributeName, obj, "edge"))
            case _ => None
          }
        }
      }

      val vertices: RDD[(VertexId, VertexData)] = createIMDBVertices(parsedTriplets)
      val edges: RDD[Edge[String]] = createIMDBEdges(parsedTriplets)
      edges.cache()

      val selectedEdgesRDD = if (previousSelectedEdges.isEmpty) {
        // 从筛选后的边中随机选择
        val initialSelectedEdgesArray = edges.takeSample(withReplacement = false, edgeSize.toInt)

        spark.sparkContext.parallelize(initialSelectedEdgesArray)
      } else {
        // 后续图：尽量保留之前选定的边
        selectEdgesToKeep(edges, previousSelectedEdges, edgeSize.toInt)
      }
      // Update previousSelectedEdges with the latest selected edges
      previousSelectedEdges = selectedEdgesRDD

      // 创建新图，仅包含选定的边和相关联的顶点
      val edgeVertices = selectedEdgesRDD
        .flatMap(e => Iterator(e.srcId, e.dstId))
        .distinct()
        .map(id => (id, ()))

      val filteredVertices = vertices
        .join(edgeVertices)
        .mapValues(_._1)

      val graph = Graph(filteredVertices, selectedEdgesRDD)
      val nonNullVertices = graph.vertices.filter { case (_, vertexData) => vertexData != null && vertexData.vertexType != null }
      val nonNullEdges = graph.edges.filter { case Edge(srcId, dstId, attr) => attr != null }

      println(s"Number of Valid vertices: ${graph.vertices.count()}, Number of Valid edges: ${graph.edges.count()}")

      // 计算所有顶点的入度
      val vertexInDegrees = nonNullEdges.map(e => (e.dstId, 1)).reduceByKey(_ + _)

      // 标记类型为movie的顶点
      val movieVertices = nonNullVertices.filter { case (_, vertexData) => vertexData.vertexType == "movie" }
      val movieVertexIds = movieVertices.map(_._1).collect().toSet

      // 筛选符合入度要求的movie顶点和超过入度要求的movie顶点
      val invalidMovieVertexIds = vertexInDegrees.filter { case (vertexId, count) => count > 100 && movieVertexIds.contains(vertexId) }.keys.collect().toSet

      // 删除与入度超过100的movie顶点相关的边
      val validEdges = nonNullEdges.filter(e => !invalidMovieVertexIds.contains(e.srcId) && !invalidMovieVertexIds.contains(e.dstId))

      // 使用过滤后的顶点和边构建新图
      val validVertices = nonNullVertices.filter { case (vertexId, _) => !invalidMovieVertexIds.contains(vertexId) }
      val newGraph = Graph(validVertices, validEdges)
      println(s"Number of filter vertices: ${newGraph.vertices.count()}, Number of filter edges: ${newGraph.edges.count()}")

      val vertexTypeCount = nonNullVertices.map(_._2.vertexType).countByValue()

      // Logging the results
      vertexTypeCount.foreach { case (vertexType, count) =>
        println(s"Vertex Type: $vertexType, Count: $count")
      }

      // Count predicates
      val predicateCounts = countPredicates(newGraph.edges)

      // Log predicate counts
      predicateCounts.collect().foreach { case (predicate, count) =>
        println(s"Predicate: $predicate, Count: $count")
      }

      val outputFilePath = new Path(outputDir, fileName).toString

      val vertexLines = validVertices.map { case (_, vertexData: VertexData) =>
        if (vertexData != null && vertexData.vertexType != null && vertexData.uri != null) {
          val uri = s"<http://imdb.org/${vertexData.vertexType}/${vertexData.uri}>"
          vertexData.attributes.toSeq.flatMap { case (attrName, attrValue) =>
            if (attrName != null && attrValue != null) {
              var mutableAttrValue = attrValue

              if (mutableAttrValue.endsWith("\\") || mutableAttrValue.endsWith(" \\")) {
                // Regular expression to remove a backslash or space followed by a backslash at the end of the string
                val cleanedLiteral = mutableAttrValue
                  .replaceAll(" \\\\$", "") // Removes ' \' if it's at the end of the string
                  .replaceAll("\\\\$", "") // Removes '\' if it's at the end of the string
                mutableAttrValue = cleanedLiteral
              }

              Some(s"""$uri <http://xmlns.com/foaf/0.1/$attrName> "$mutableAttrValue" .""")
            } else None
          }.mkString("\n")
        } else ""
      }.filter(_.nonEmpty)

      // 创建映射边到它们的源顶点和目标顶点
      val vertexRdd = newGraph.vertices

      // 将边与其源顶点和目标顶点的数据进行连接
      val edgesWithVertices = newGraph.edges
        .map(e => (e.srcId, e))
        .join(vertexRdd)
        .map { case (_, (edge, srcVertexData)) => (edge.dstId, (edge, srcVertexData)) }
        .join(vertexRdd)
        .map { case (_, ((edge, srcVertexData), dstVertexData)) => (edge, srcVertexData, dstVertexData) }

      val edgeLines = edgesWithVertices.flatMap { case (edge, srcVertexData, dstVertexData) =>
        if (srcVertexData != null && srcVertexData.vertexType != null && srcVertexData.uri != null &&
          dstVertexData != null && dstVertexData.vertexType != null && dstVertexData.uri != null) {
          val srcUri = s"<http://imdb.org/${srcVertexData.vertexType}/${srcVertexData.uri}>"
          val dstUri = s"<http://imdb.org/${dstVertexData.vertexType}/${dstVertexData.uri}>"
          Some(s"$srcUri <http://xmlns.com/foaf/0.1/${edge.attr}> $dstUri .")
        } else {
          None
        }
      }

      // 计算顶点和边的数量
      val vertexCount = vertexLines.count()
      val edgeCount = edgeLines.count()

      println(s"Number of vertices: $vertexCount")
      println(s"Number of edges: $edgeCount")

      // 合并顶点和边的数据
      val graphData = vertexLines.union(edgeLines)

      // 写入到HDFS
      graphData.saveAsTextFile(outputFilePath)
    }
    println("FINISHED!!!")
    spark.stop()
  }

  def selectEdgesToKeep(currentEdges: RDD[Edge[String]], previousSelectedEdges: RDD[Edge[String]], desiredSize: Int): RDD[Edge[String]] = {
    val sc = currentEdges.sparkContext
    val currentSelectedEdges = currentEdges.intersection(previousSelectedEdges)
    val additionalEdgesNeeded = desiredSize - currentSelectedEdges.count().toInt
    val additionalEdges = if (additionalEdgesNeeded > 0) {
      currentEdges.subtract(currentSelectedEdges).takeSample(withReplacement = false, additionalEdgesNeeded)
    } else {
      Array[Edge[String]]()
    }
    sc.parallelize(currentSelectedEdges.collect() ++ additionalEdges)
  }
}
