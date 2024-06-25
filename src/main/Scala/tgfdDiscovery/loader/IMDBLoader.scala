package tgfdDiscovery.loader

import org.apache.log4j.Logger

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.script.IMDBGraphUtils.{attributeRegex, createIMDBEdges, createIMDBVertices, isDesiredType, literalRegex, uriRegex}
import tgfdDiscovery.common.VertexData

object IMDBLoader {
  val logger = Logger.getLogger(this.getClass.getName)

  def loadGraphSnapshots(spark: SparkSession, hdfsBasePath: String, startDateStr: String, endDateStr: String): Seq[Graph[VertexData, String]] = {
    val formatter = DateTimeFormatter.ofPattern("yyMMdd")
    val startDate = LocalDate.parse(startDateStr, formatter)
    val endDate = LocalDate.parse(endDateStr, formatter)

    val snapshotFileNames = Iterator.iterate(startDate)(_.plusMonths(1))
      .takeWhile(!_.isAfter(endDate))
      .map(date => s"imdb-${date.format(formatter)}.nt")
      .toList

    var graphs: Seq[Graph[VertexData, String]] = Seq()

    snapshotFileNames.foreach { fileName =>
      val filePath = hdfsBasePath + fileName
      println(s"Processing file: $filePath")
      val fileContent = spark.sparkContext.textFile(filePath)
      val (vertices, edges) = parseFile(fileContent)

      val graph = Graph(vertices, edges)
      graphs = graphs :+ graph
    }

    graphs
  }

  private def parseFile(fileContent: RDD[String]): (RDD[(VertexId, VertexData)], RDD[Edge[String]]) = {
    val desiredTypes = Set("movie", "actor", "director", "country", "actress", "genre")

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
    println(s"Created ${vertices.count()} vertices and ${edges.count()} edges.")

    (vertices, edges)
  }
}
