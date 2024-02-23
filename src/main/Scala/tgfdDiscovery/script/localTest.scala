package tgfdDiscovery.script

import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData
import tgfdDiscovery.script.finalLoader.{createEdges, extractType, extractVertexURI}

import scala.collection.mutable
import scala.util.matching.Regex

object localTest {
  val logger = Logger.getLogger(this.getClass.getName)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val typeRegex: Regex = ".*/([^/]+)/[^/]*>$".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("RDF Graph Loader")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val localFilePath = "/Users/roy/Desktop/TGFD/datasets/imdb/1m_imdb_old/imdb-180909.nt"
    val fileContent = spark.sparkContext.textFile(localFilePath)

    val parsedTriplets = fileContent.flatMap { line =>
      Option(line).map(_.trim.dropRight(1).replaceAll("""\^\^<http://www\.w3\.org/2001/XMLSchema#(float|integer)>""", "")).flatMap { trimmedLine =>
        val uriMatches = uriRegex.findAllIn(trimmedLine).toList
        val literalMatches = literalRegex.findAllIn(trimmedLine).toList

        (uriMatches, literalMatches) match {
          case (List(subject, predicate), List(literal)) =>
//            logger.info("subject: " + subject)
            val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
            Some((subject, attributeName, literal.replaceAll("\"", ""), "attribute"))
          case (List(subject, predicate, obj), _) =>
//            logger.info(s"subject: $subject, obj: $obj")
            val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
            Some((subject, attributeName, obj, "edge"))
          case _ => None
        }
      }
    }
//    val tripletsCollected = parsedTriplets.collect() // 或者 parsedTriplets.take(n) 来取前n条记录

    val vertices: RDD[(VertexId, VertexData)] = createVertices(parsedTriplets)
    val edges: RDD[Edge[String]] = createEdges(parsedTriplets)
    val graph = Graph(vertices, edges)
//    val vCount = vertices.count()
//    val eCount = edges.count()
//    println(s"There are ${eCount} edges and ${vCount} vertices in the graph.")

    // 统计每种类型的边出现的次数
    val edgeFrequency = edges
      .map(edge => (edge.attr, 1)) // 将每条边映射为(边类型, 1)
      .reduceByKey(_ + _) // 按边类型聚合并计数
      .cache() // 缓存结果，因为我们要执行多个动作

    // 找到出现次数最多的10种边类型
    val mostFrequentEdges = edgeFrequency.top(10)(Ordering.by(_._2))

    println("Most frequent 10 edges:")
    mostFrequentEdges.foreach { case (edgeType, count) =>
      println(s"Edge type: $edgeType, Count: $count")
    }


    // 计算每个顶点的度数
    val vertexDegrees = graph.degrees.cache() // 缓存结果，因为我们要执行多个动作

    // 找到度数最高的10个顶点
    val mostFrequentVertices = vertexDegrees.top(10)(Ordering.by(_._2))

    println("Most frequent 10 vertices:")

    // 首先，将mostFrequentVertices转换为一个RDD，因为我们需要执行join操作
    // 注意：mostFrequentVertices是一个Array，我们需要将其转换为RDD
    val mostFrequentVerticesRDD = spark.sparkContext.parallelize(mostFrequentVertices)
    mostFrequentVerticesRDD.take(5).foreach(println)
    vertices.take(5).foreach(println)

    // 现在，使用join操作将这个RDD与vertices RDD结合起来，以获取VertexData
    val vertexIdWithDegreeAndData = mostFrequentVerticesRDD.join(vertices)
    val number = vertexIdWithDegreeAndData.count()
    println(s"Number of vertices: $number")
    // 打印出每个顶点的详细信息和度数
    vertexIdWithDegreeAndData.foreach { case (vertexId, (degree, vertexData)) =>
      println(s"Vertex ID: $vertexId, Degree: $degree, Vertex Data: ${vertexData.uri}, Vertex Type: ${vertexData.vertexType}")
    }


  }

  def createVertices(parsedTriplets: RDD[(String, String, String, String)]): RDD[(VertexId, VertexData)] = {
    parsedTriplets
      .filter(_._4 == "attribute")
      .map { case (subj, attrName, attrValue, _) =>
        val subURI = extractVertexURI(subj)
        val vertexType = extractType(subj)
        ((subURI, vertexType), mutable.HashMap(attrName -> attrValue))
      }
      .reduceByKey { (attrs1, attrs2) =>
        attrs1 ++= attrs2
      }
      .map { case ((uri, vertexType), attributes) =>
//        val id = (uri + vertexType).hashCode.toLong
        (uri.hashCode.toLong, VertexData(uri = uri, vertexType = vertexType, attributes = attributes))
      }
  }
}
