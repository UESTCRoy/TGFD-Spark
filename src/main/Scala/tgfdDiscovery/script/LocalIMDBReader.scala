package tgfdDiscovery.script

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.IMDBGraphUtils.{createIMDBEdges, createIMDBVertices, isDesiredType}
import tgfdDiscovery.common.VertexData

import scala.util.matching.Regex

object LocalIMDBReader {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  // 正则表达式定义
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val attributeRegex: Regex = "/([^/>]+)>$".r

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("RDF Graph Loader")
      .master("local[*]")
      .getOrCreate()

    val inputDir = "/Users/roy/Desktop/TGFD/datasets/imdb/300k_imdb_split4/300k_4/imdb3-170909.nt"

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

      // Counting the vertices and edges
      val vertexTypeCount = vertices.map(_._2.vertexType).countByValue()

      // Logging the results
      vertexTypeCount.foreach { case (vertexType, count) =>
        println(s"Vertex Type: $vertexType, Count: $count")
      }

      // Count predicates
      val predicateCounts = countPredicates(edges)

      // Log predicate counts
      predicateCounts.collect().foreach { case (predicate, count) =>
        println(s"Predicate: $predicate, Count: $count")
      }

    }

    spark.stop()
  }

  def countPredicates(edges: RDD[Edge[String]]): RDD[(String, Int)] = {
    edges.map(edge => (edge.attr, 1)).reduceByKey(_ + _)
  }
}
