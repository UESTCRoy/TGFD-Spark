import finalLoader.{createEdges, createVertices}
import org.apache.log4j.Logger
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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

//    spark.sparkContext.setLogLevel("WARN")

    val localFilePath = "/Users/roy/Desktop/TGFD/datasets/imdb/imdb-test/imdb-170909.nt"
    val fileContent = spark.sparkContext.textFile(localFilePath)

    val parsedTriplets = fileContent.flatMap { line =>
      Option(line).map(_.trim.dropRight(1).replaceAll("""\^\^<http://www\.w3\.org/2001/XMLSchema#(float|integer)>""", "")).flatMap { trimmedLine =>
        val uriMatches = uriRegex.findAllIn(trimmedLine).toList
        val literalMatches = literalRegex.findAllIn(trimmedLine).toList

        (uriMatches, literalMatches) match {
          case (List(subject, predicate), List(literal)) =>
            logger.info("subject: " + subject)
            val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
            Some((subject, attributeName, literal.replaceAll("\"", ""), "attribute"))
          case (List(subject, predicate, obj), _) =>
            logger.info(s"subject: $subject, obj: $obj")
            val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
            Some((subject, attributeName, obj, "edge"))
          case _ => None
        }
      }
    }
    val tripletsCollected = parsedTriplets.collect() // 或者 parsedTriplets.take(n) 来取前n条记录

//    val vertices: RDD[(VertexId, VertexData)] = createVertices(parsedTriplets)
//    val edges: RDD[Edge[String]] = createEdges(parsedTriplets)
//    val graph = Graph(vertices, edges)
  }
}
