package tgfdDiscovery.script

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import IMDBGraphUtils.{createIMDBEdges, createIMDBVertices}
import tgfdDiscovery.common.VertexData
import tgfdDiscovery.script.GraphPartitionByEdge.{attributeRegex, literalRegex, logger, uriRegex}

import scala.util.matching.Regex

object GraphPartitionByMovieVertexIMDB {
  val logger = Logger.getLogger(this.getClass.getName)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val typeRegex: Regex = ".*/([^/]+)/[^/]*>$".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 3) {
      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("IMDB Graph Partitioner")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputFilePath = args(0)
    val outputDir = args(1)
    val numPartitions = args(2).toInt

    val fileName = new Path(inputFilePath).getName
    println(s"Processing file: $inputFilePath")

    val fileContent = spark.sparkContext.textFile(inputFilePath)

    // 解析文件内容
    val parsedTriplets = fileContent.flatMap { line =>
      Option(line).map(_.trim.dropRight(1).replaceAll("""\^\^<http://www\.w3\.org/2001/XMLSchema#(float|integer)>""", "")).flatMap { trimmedLine =>
        val uriMatches = uriRegex.findAllIn(trimmedLine).toList
        val literalMatches = literalRegex.findAllIn(trimmedLine).toList

        (uriMatches, literalMatches) match {
          case (List(subject, predicate), List(literal)) =>
            val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
            Some((subject, attributeName, literal.replaceAll("\"", ""), "attribute"))
          case (List(subject, predicate, obj), _) =>
            val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
            Some((subject, attributeName, obj, "edge"))
          case _ => None
        }
      }
    }

    val vertices: RDD[(VertexId, VertexData)] = createIMDBVertices(parsedTriplets)
    val edges: RDD[Edge[String]] = createIMDBEdges(parsedTriplets)
    val graph = Graph(vertices, edges)
    println(s"Original: Number of vertices: ${graph.vertices.count()}, Number of edges: ${graph.edges.count()}")
    println("====================================")

    // Distribute movie type vertices evenly across partitions
    val movieVertices = vertices.filter { case (_, vData) => vData.vertexType == "movie" }
    val partitionedMovieVertices = movieVertices.map { case (vertexId, vertexData) =>
      val hash = vertexId.hashCode()
      (hash % numPartitions, (vertexId, vertexData))
    }.groupByKey(numPartitions)

    // Process each partition to form subgraphs
    partitionedMovieVertices.collect().foreach { case (partitionId, verticesInPartition) =>
      val vertexIdsInPartition = verticesInPartition.map(_._1).toSet
      val edgesInPartition = edges.filter(edge => vertexIdsInPartition.contains(edge.srcId) || vertexIdsInPartition.contains(edge.dstId))

      // Collect additional vertex IDs from the edges
      val additionalVertexIds = edgesInPartition.flatMap(edge => Seq(edge.srcId, edge.dstId)).distinct().collect().toSet

      // Fetch all vertices that are either in the original partition or connected via an edge
      val allVertexIds = vertexIdsInPartition ++ additionalVertexIds
      val allVerticesInPartition = vertices.filter { case (vertexId, _) => allVertexIds.contains(vertexId) }

      // Create an RDD for vertices and edges to form a subgraph
      val vertexRDD = spark.sparkContext.parallelize(allVerticesInPartition.collect())
      val edgeRDD = spark.sparkContext.parallelize(edgesInPartition.collect())

      // Construct the subgraph using the vertices and edges for this partition
      val subGraph = Graph(vertexRDD, edgeRDD)

      println(s"Processing $fileName, partition $partitionId")
      println(s"Number of vertices: ${subGraph.vertices.count()}, Number of edges: ${subGraph.edges.count()}")

      // 保存子图
      val subGraphFileName = {
        val hyphenIndex = fileName.indexOf("-")
        if (hyphenIndex != -1) {
          val baseName = fileName.substring(0, hyphenIndex)
          val remaining = fileName.substring(hyphenIndex)
          s"${baseName}$partitionId$remaining"
        } else {
          s"${fileName}$partitionId"
        }
      }

      val outputFilePath = new Path(outputDir, subGraphFileName).toString

      val vertexLines = vertexRDD.map { case (vertexId, vertexData) =>
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
      val vertexRdd = subGraph.vertices

      // 将边与其源顶点和目标顶点的数据进行连接
      val edgesWithVertices = edgeRDD
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

      println("Partitioned graph:" + subGraphFileName)
      println(s"Number of vertices: $vertexCount")
      println(s"Number of edges: $edgeCount")

      // 合并顶点和边的数据
      val graphData = vertexLines.union(edgeLines)

      // 写入到HDFS
      graphData.saveAsTextFile(outputFilePath)
      println("====================================")
    }
    println("FINISHED!!!")
    spark.stop()
  }
}
