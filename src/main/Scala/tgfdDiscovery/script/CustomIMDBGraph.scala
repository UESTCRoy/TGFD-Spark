package tgfdDiscovery.script

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.IMDBGraphUtils.{createIMDBEdges, createIMDBVertices, extractIMDBType, extractIMDBVertexURI, isDesiredType}
import tgfdDiscovery.common.VertexData

import scala.collection.mutable
import scala.util.matching.Regex

object CustomIMDBGraph {
  val logger = Logger.getLogger(this.getClass.getName)

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

    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
    val fileStatusList = fs.listStatus(new Path(inputDir))

    var previousSelectedEdges: Set[Edge[String]] = Set()

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
        val graph = Graph(vertices, edges)
        val inDegrees: RDD[(VertexId, Int)] = graph.inDegrees
        val outDegrees: RDD[(VertexId, Int)] = graph.outDegrees

        val degrees: RDD[(VertexId, (Int, Int))] = inDegrees.fullOuterJoin(outDegrees)
          .mapValues {
            case (inOpt, outOpt) => (inOpt.getOrElse(0), outOpt.getOrElse(0))
          }

        // 筛选出入度和出度都大于一的顶点
        val validVertices = degrees
          .filter { case (_, (inDeg, outDeg)) => inDeg > 1 || outDeg > 1 }
          .map(_._1)
          .collect()
          .toSet

        // 根据有效顶点筛选边
        val validEdges = graph.edges
          .filter(e => validVertices.contains(e.srcId) && validVertices.contains(e.dstId))

        // 从筛选后的边中随机选择
        val selectedEdgesArray = validEdges.takeSample(withReplacement = false, edgeSize.toInt)

        previousSelectedEdges = selectedEdgesArray.toSet
        spark.sparkContext.parallelize(selectedEdgesArray)
      } else {
        // 后续图：尽量保留之前选定的边
        selectEdgesToKeep(edges, previousSelectedEdges, edgeSize.toInt)
      }

      // 创建新图，仅包含选定的边和相关联的顶点
      val edgeVertices = selectedEdgesRDD
        .flatMap(e => Iterator(e.srcId, e.dstId))
        .distinct()
        .map(id => (id, ()))

      val filteredVertices = vertices
        .join(edgeVertices)
        .mapValues(_._1)

      val newGraph = Graph(filteredVertices, selectedEdgesRDD)

      println(s"Number of Valid vertices: ${newGraph.vertices.count()}, Number of Valid edges: ${newGraph.edges.count()}")

      val outputFilePath = new Path(outputDir, fileName).toString

      // 转换顶点数据为字符串
      val vertexLines = filteredVertices.map { case (_, vertexData: VertexData) =>
        if (vertexData != null && vertexData.vertexType != null && vertexData.uri != null) {
          val uri = s"<http://imdb.org/${vertexData.vertexType}/${vertexData.uri}>"
          vertexData.attributes.toSeq.flatMap { case (attrName, attrValue) =>
            if (attrName != null && attrValue != null) {
              Some(s"""$uri <http://xmlns.com/foaf/0.1/$attrName> "$attrValue" .""")
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

  def selectEdgesToKeep(currentEdges: RDD[Edge[String]], previousSelectedEdges: Set[Edge[String]], desiredSize: Int): RDD[Edge[String]] = {
    val sc = currentEdges.sparkContext
    val currentSelectedEdges = currentEdges.filter(previousSelectedEdges.contains)
    val additionalEdgesNeeded = desiredSize - currentSelectedEdges.count().toInt
    val additionalEdges = if (additionalEdgesNeeded > 0) {
      currentEdges.subtract(currentSelectedEdges).takeSample(withReplacement = false, additionalEdgesNeeded)
    } else {
      Array[Edge[String]]()
    }
    sc.parallelize(currentSelectedEdges.collect() ++ additionalEdges)
  }
}
