package tgfdDiscovery.script.dbpedia

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData
import tgfdDiscovery.script.dbpedia.DBPediaGraphUtils.{createDBPediaEdges, createDBPediaVertices}

import scala.collection.mutable
import scala.util.matching.Regex

object DBPediaPartitionByEdge {
  val logger = Logger.getLogger(this.getClass.getName)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    if (args.length < 3) {
      logger.error("Usage: DBPediaLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("DBPedia Graph Partitioner By Edges")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)
    val numPartitions = args(2).toInt

    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
    val fileStatusList = fs.listStatus(new Path(inputDir))

    fileStatusList.foreach { fileStatus =>
      val filePath = fileStatus.getPath.toString
      val fileName = fileStatus.getPath.getName
      val fileContent = spark.sparkContext.textFile(filePath)
      println(s"Processing file: $filePath")

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

      val vertices: RDD[(VertexId, VertexData)] = createDBPediaVertices(parsedTriplets)
      val edges: RDD[Edge[String]] = createDBPediaEdges(parsedTriplets)
      val graph = Graph(vertices, edges)

      val initialEdgePredCounts = graph.edges
        .map(_.attr)
        .distinct()
        .collect()
        .map(pred => (pred, Array.fill(numPartitions)(0)))
        .toMap

      val edgePredCounts = mutable.Map(initialEdgePredCounts.toSeq: _*)

      val edgesWithPartition = graph.edges.map { edge =>
        val partitionId = assignEdgePartition(edge.attr, numPartitions, edgePredCounts)
        (edge, partitionId)
      }

      for (partitionId <- 0 until numPartitions) {
        // 过滤属于当前分区的边
        val edgesInPartition = edgesWithPartition.filter(_._2 == partitionId).map(_._1)

        // 获取当前分区内的顶点ID
        val vertexIdsInPartition = edgesInPartition.flatMap(edge => Seq(edge.srcId, edge.dstId)).distinct()

        // 通过 join 获取顶点数据
        val verticesInPartition = vertexIdsInPartition
          .map(id => (id, id))
          .join(graph.vertices)
          .map { case (_, (id, data)) => (id, data) }

        val subGraph = Graph(verticesInPartition, edgesInPartition)

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

        val vertexLines = verticesInPartition.map { case (vertexId, vertexData) =>
          if (vertexData != null && vertexData.vertexType != null && vertexData.uri != null) {
            val uri = s"<http://dbpedia.org/${vertexData.vertexType}/${vertexData.uri}>"
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
        val edgesWithVertices = edgesInPartition
          .map(e => (e.srcId, e))
          .join(vertexRdd)
          .map { case (_, (edge, srcVertexData)) => (edge.dstId, (edge, srcVertexData)) }
          .join(vertexRdd)
          .map { case (_, ((edge, srcVertexData), dstVertexData)) => (edge, srcVertexData, dstVertexData) }

        val edgeLines = edgesWithVertices.flatMap { case (edge, srcVertexData, dstVertexData) =>
          if (srcVertexData != null && srcVertexData.vertexType != null && srcVertexData.uri != null &&
            dstVertexData != null && dstVertexData.vertexType != null && dstVertexData.uri != null) {
            val srcUri = s"<http://dbpedia.org/${srcVertexData.vertexType}/${srcVertexData.uri}>"
            val dstUri = s"<http://dbpedia.org/${dstVertexData.vertexType}/${dstVertexData.uri}>"
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
    }
    println("FINISHED!!!")
    spark.stop()
  }

  // 分配边到不同分区的函数
  def assignEdgePartition(pred: String, numPartitions: Int, edgePredCounts: mutable.Map[String, Array[Int]]): Int = {
    // 获取该pred的当前分区计数
    val counts = edgePredCounts.getOrElseUpdate(pred, Array.fill(numPartitions)(0))
    // 选择当前最少的分区
    val partitionId = counts.zipWithIndex.minBy(_._1)._2
    // 更新计数
    counts(partitionId) += 1
    partitionId
  }
}
