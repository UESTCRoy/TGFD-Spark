package tgfdDiscovery.script.dbpedia

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData
import tgfdDiscovery.script.dbpedia.DBPediaGraphUtils.{createDBPediaEdges, createDBPediaVertices}

import scala.util.matching.Regex

object CustomDBPediaGraph {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      logger.error("Usage: DBPediaLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("Generate Custom DBPedia Graph")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)
    val edgeSize = args(2)
    val filterDegree = args(3).toInt

    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
    val fileStatusList = fs.listStatus(new Path(inputDir))

    var previousSelectedEdges: RDD[Edge[String]] = spark.sparkContext.emptyRDD[Edge[String]]

    fileStatusList.foreach { fileStatus =>
      val filePath = fileStatus.getPath.toString
      val fileName = fileStatus.getPath.getName
      val fileContent = spark.sparkContext.textFile(filePath)
      println(s"Processing file: $filePath")

      val parsedTriplets = fileContent.flatMap { line =>
        Option(line).map(_.trim.dropRight(1)).flatMap { trimmedLine =>
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

      val rawVertices: RDD[(VertexId, VertexData)] = createDBPediaVertices(parsedTriplets)
      val rawEdges: RDD[Edge[String]] = createDBPediaEdges(parsedTriplets)
      val rawGraph = Graph(rawVertices, rawEdges)
      println(s"Number of Raw Vertices: ${rawGraph.vertices.count()}, Number of Raw Edges: ${rawGraph.edges.count()}")

      // Calculate in-degrees and out-degrees
      val degrees = rawGraph.degrees.cache()
      val highDegreeVertices = degrees.filter { case (_, degree) => degree > filterDegree }
      val invalidVertexIds = highDegreeVertices.keys.collect().toSet
      println(s"Number of High Degree Vertices: ${invalidVertexIds.size}")

      // Create Non Null Graph
      val vertexIdsFromEdges: RDD[(VertexId, Unit)] = rawEdges
        .flatMap(edge => Iterator((edge.srcId, ()), (edge.dstId, ())))
        .distinct()

      val connectedVertices: RDD[(VertexId, VertexData)] = rawVertices
        .join(vertexIdsFromEdges)
        .map {
          case (id, (vertexData, _)) => (id, vertexData)
        }
        .filter { case (id, _) => !invalidVertexIds.contains(id) }

      val connectedVertexIds = connectedVertices.keys.collect().toSet
      println(s"Number of Low Degree Connected Vertex Ids: ${connectedVertexIds.size}")
      val bcConnectedVertexIds = spark.sparkContext.broadcast(connectedVertexIds)

      val connectedEdges = rawEdges.filter(edge =>
        bcConnectedVertexIds.value.contains(edge.srcId) && bcConnectedVertexIds.value.contains(edge.dstId))

      val connectedGraph = Graph(connectedVertices, connectedEdges)
      println(s"Number of Connected Vertices: ${connectedGraph.vertices.count()}, Number of Connected Edges: ${connectedGraph.edges.count()}")

      // Calculate in-degrees and out-degrees
//      val degrees = rawGraph.degrees.cache()
//
//      val highDegreeVertices = degrees.filter { case (_, degree) => degree > filterDegree }
//      val invalidVertexIds = highDegreeVertices.keys.collect().toSet
//
//      val filteredVertices = rawVertices.filter { case (id, _) => !invalidVertexIds.contains(id) }
//      val validVertexIds = filteredVertices.keys.collect().toSet
//      val filteredEdges = rawEdges.filter(e => validVertexIds.contains(e.srcId) && validVertexIds.contains(e.dstId))
//
//      val filteredGraph = Graph(filteredVertices, filteredEdges)
//      println(s"Number of Filtered Vertices: ${filteredGraph.vertices.count()}, Number of Filtered Edges: ${filteredGraph.edges.count()}")

      val selectedEdgesRDD = if (previousSelectedEdges.isEmpty) {
        // 从筛选后的边中随机选择
        val initialSelectedEdgesArray = connectedEdges.takeSample(withReplacement = false, edgeSize.toInt)

        spark.sparkContext.parallelize(initialSelectedEdgesArray)
      } else {
        // 后续图：尽量保留之前选定的边
        selectEdgesToKeep(connectedEdges, previousSelectedEdges, edgeSize.toInt)
      }
      // Update previousSelectedEdges with the latest selected edges
      previousSelectedEdges = selectedEdgesRDD

      // 创建新图，仅包含选定的边和相关联的顶点
      val selectedEdgeVertices = selectedEdgesRDD
        .flatMap(e => Iterator(e.srcId, e.dstId))
        .distinct()
        .map(id => (id, ()))

      val selectedVertices = connectedVertices
        .join(selectedEdgeVertices)
        .mapValues(_._1)

      val customGraph = Graph(selectedVertices, selectedEdgesRDD)
      println(s"Number of Raw Custom vertices: ${selectedVertices.count()}, Number of Raw Custom edges: ${selectedEdgesRDD.count()}")
      println(s"Number of Custom vertices: ${customGraph.vertices.count()}, Number of Custom edges: ${customGraph.edges.count()}")

      val vertexTypeCount = selectedVertices.map(_._2.vertexType).countByValue()

      // Convert the Map to a Seq and sort by count
      val sortedVertexTypeCount = vertexTypeCount.toSeq.sortBy(-_._2)  // Negative sign for descending order

      // Print the sorted results
      sortedVertexTypeCount.foreach { case (vertexType, count) =>
//        println(s"Vertex Type: $vertexType, Count: $count")
      }

      val predicateCounts = countPredicates(customGraph.edges)

      // Sort by count in descending order and collect to the driver
      val sortedPredicateCounts = predicateCounts.sortBy(_._2, ascending = false).collect()

      // Print the sorted results
      sortedPredicateCounts.foreach { case (predicate, count) =>
//        println(s"Predicate: $predicate, Count: $count")
      }

      val outputFilePath = new Path(outputDir, fileName).toString

      val vertexLines = selectedVertices.map { case (_, vertexData: VertexData) =>
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

      // 将边与其源顶点和目标顶点的数据进行连接
      val edgesWithVertices = selectedEdgesRDD
        .map(e => (e.srcId, e))
        .join(selectedVertices)
        .map { case (_, (edge, srcVertexData)) => (edge.dstId, (edge, srcVertexData)) }
        .join(selectedVertices)
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

  def countPredicates(edges: RDD[Edge[String]]): RDD[(String, Int)] = {
    edges.map(edge => (edge.attr, 1)).reduceByKey(_ + _)
  }
}
