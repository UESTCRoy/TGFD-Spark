package tgfdDiscovery.script.dbpedia

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData
import tgfdDiscovery.script.dbpedia.CustomDBPediaGraph.{attributeRegex, literalRegex, uriRegex}
import tgfdDiscovery.script.dbpedia.DBPediaGraphUtils.{createDBPediaEdges, createDBPediaVertices}

object DBPediaVertexDegree {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Calculate Vertex Degree")
      .getOrCreate()

    val filePath = args(0)
    val fileContent = spark.sparkContext.textFile(filePath)

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

    // Create Non Null Graph
    val vertexIdsFromEdges: RDD[(VertexId, Unit)] = rawEdges
      .flatMap(edge => Iterator((edge.srcId, ()), (edge.dstId, ())))
      .distinct()

    val connectedVertices: RDD[(VertexId, VertexData)] = rawVertices
      .join(vertexIdsFromEdges)
      .map {
        case (id, (vertexData, _)) => (id, vertexData)
      }

    val connectedVertexIds = connectedVertices.keys.collect().toSet
    val bcConnectedVertexIds = spark.sparkContext.broadcast(connectedVertexIds)

    val connectedEdges = rawEdges.filter(edge =>
      bcConnectedVertexIds.value.contains(edge.srcId) && bcConnectedVertexIds.value.contains(edge.dstId))

    val connectedGraph = Graph(connectedVertices, connectedEdges)
    println(s"Number of Connected Vertices: ${connectedGraph.vertices.count()}, Number of Connected Edges: ${connectedGraph.edges.count()}")

    // Calculate degree distributions
    val inDegreeDistribution = calculateDegreeDistribution(connectedGraph.inDegrees)
    val outDegreeDistribution = calculateDegreeDistribution(connectedGraph.outDegrees)

    // Print results
    println("In-degree distribution:")
    inDegreeDistribution.foreach { case (range, count) =>
      println(s"$range: $count")
    }

    println("Out-degree distribution:")
    outDegreeDistribution.foreach { case (range, count) =>
      println(s"$range: $count")
    }

  }

  def calculateDegreeDistribution(degreesRDD: RDD[(VertexId, Int)], bucketSize: Int = 100, maxDegree: Int = 1000): Array[(String, Int)] = {
    // Create degree ranges
    val buckets = (0 until maxDegree by bucketSize).map(start => s"${start}-${start + bucketSize - 1}")

    // Count degrees in each bucket
    val bucketCounts = degreesRDD.map { case (_, degree) =>
        val bucketIndex = math.min(degree / bucketSize, maxDegree / bucketSize - 1)
        (buckets(bucketIndex), 1)
      }
      .reduceByKey(_ + _)
      .collect()

    bucketCounts
  }
}
