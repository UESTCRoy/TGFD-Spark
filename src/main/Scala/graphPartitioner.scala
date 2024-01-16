import finalLoader.{createEdges, createVertices}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.matching.Regex

object graphPartitioner {
  val logger = Logger.getLogger(this.getClass.getName)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val typeRegex: Regex = ".*/([^/]+)/[^/]*>$".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("RDF Graph Loader")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)

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

      val vertices: RDD[(VertexId, VertexData)] = createVertices(parsedTriplets)
      val edges: RDD[Edge[String]] = createEdges(parsedTriplets)
      val graph = Graph(vertices, edges)

      val numPartitions = 4

      // 分配顶点到不同分区的函数
      def assignPartition(vertexData: VertexData, numPartitions: Int, vertexTypeCounts: mutable.Map[String, Array[Int]]): Int = {
        // 获取该类型顶点的当前分区计数
        val counts = vertexTypeCounts.getOrElseUpdate(vertexData.vertexType, Array.fill(numPartitions)(0))

        // 选择当前最少的分区
        val partitionId = counts.zipWithIndex.minBy(_._1)._2

        // 更新计数
        counts(partitionId) += 1

        partitionId
      }

      // 初始化分配状态
      val initialVertexTypeCounts = graph.vertices
        .map { case (_, vertexData) => vertexData.vertexType }
        .distinct()
        .collect()
        .map(vertexType => (vertexType, Array.fill(numPartitions)(0)))
        .toMap

      val vertexTypeCounts = mutable.Map(initialVertexTypeCounts.toSeq: _*)

      // 分配顶点到分区
      val verticesWithPartition = graph.vertices.map {
        case (vertexId, vertexData) =>
          (vertexId, (vertexData, assignPartition(vertexData, numPartitions, vertexTypeCounts)))
      }

      val verticesPartitionMap = verticesWithPartition.collectAsMap()

      for (partitionId <- 0 until numPartitions) {
        // 过滤属于当前分区的顶点
        val verticesInPartition = verticesWithPartition.filter(_._2._2 == partitionId).mapValues(_._1)

        // 过滤属于当前分区的边
        val edgesInPartition = graph.edges.filter { e =>
          val srcPartition = verticesPartitionMap.getOrElse(e.srcId, (-1, -1))._2
          val dstPartition = verticesPartitionMap.getOrElse(e.dstId, (-1, -1))._2
          srcPartition == partitionId && dstPartition == partitionId
        }


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

        val vertexLines = verticesInPartition.map { case (_, vertexData) =>
          if (vertexData != null && vertexData.vertexType != null && vertexData.uri != null) {
            val uri = s"<http://imdb.org/${vertexData.vertexType}/${vertexData.uri}>"
            vertexData.attributes.toSeq.flatMap { case (attrName, attrValue) =>
              if (attrName != null && attrValue != null) {
                Some(s"$uri <http://xmlns.com/foaf/0.1/$attrName> \"$attrValue\" .")
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

    }
    println("FINISHED!!!")
    spark.stop()
  }
}
