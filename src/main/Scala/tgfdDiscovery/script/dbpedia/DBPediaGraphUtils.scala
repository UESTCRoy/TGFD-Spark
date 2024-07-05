package tgfdDiscovery.script.dbpedia

import org.apache.jena.rdf.model.Model
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter
import scala.util.matching.Regex

// Two Version:
// 1. VertexData -> (String, List[String]) for script
// 2. VertexData -> VertexData for spark mining
object DBPediaGraphUtils {
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val attributeRegex: Regex = "/([^/>]+)>$".r
  val dbpediaUriRegex: Regex = "<http://dbpedia.org/([^/]+)/([^>]+)>".r
  
  //  def createGraph(model: Model, spark: SparkSession): Graph[VertexData, String] = {
  def createGraph(model: Model, spark: SparkSession): Graph[(String, List[String]), String] = {
    val vertices: RDD[(VertexId, (String, List[String]))] = spark.sparkContext.parallelize(
      model.listSubjects().asScala.filter(_.isURIResource).map { resource =>
        val uri = resource.getURI
        val id = uri.hashCode.toLong
        val properties = model.listStatements(resource, null, null).asScala
          .map(stmt => stmt.toString).toList
        (id, (uri, properties))
      }.toList
    )

    // Force materialization to debug
    val vertexCount = vertices.count()
    println(s"Total vertices loaded: $vertexCount")

    val validVertexIds = vertices.map(_._1).collect().toSet

    val edges: RDD[Edge[String]] = spark.sparkContext.parallelize(
      model.listStatements().asScala.filter(stmt => stmt.getObject.isResource &&
        validVertexIds.contains(stmt.getSubject.getURI.hashCode.toLong) &&
        validVertexIds.contains(stmt.getObject.asResource.getURI.hashCode.toLong)
      ).map { stmt =>
        val srcId = stmt.getSubject.getURI.hashCode.toLong
        val dstId = stmt.getObject.asResource.getURI.hashCode.toLong
        Edge(srcId, dstId, stmt.getPredicate.getURI)
      }.toList
    )

    // Force materialization to debug
    val edgeCount = edges.count()
    println(s"Total edges loaded: $edgeCount")

    Graph(vertices, edges)
  }

  def createDBPediaVertices(parsedTriplets: RDD[(String, String, String, String)]): RDD[(VertexId, VertexData)] = {
    parsedTriplets
      .filter(_._4 == "attribute")
      .map { case (subj, attrName, attrValue, _) =>
        val subURI = extractDBPediaVertexURI(subj)
        val vertexType = extractDBPediaType(subj)
        ((subURI, vertexType), mutable.HashMap(attrName -> attrValue))
      }
      .reduceByKey { (attrs1, attrs2) =>
        attrs1 ++= attrs2
      }
      .map { case ((uri, vertexType), attributes) =>
        val id = (uri + vertexType).hashCode.toLong
        (id, VertexData(uri = uri, vertexType = vertexType, attributes = attributes))
      }
  }

  def createDBPediaEdges(parsedTriplets: RDD[(String, String, String, String)]): RDD[Edge[String]] = {
    parsedTriplets
      .filter(_._4 == "edge")
      .map { case (subj, pred, obj, _) =>
        val subURI = extractDBPediaVertexURI(subj)
        val subType = extractDBPediaType(subj)
        val objURI = extractDBPediaVertexURI(obj)
        val objType = extractDBPediaType(obj)
        Edge((subURI + subType).hashCode.toLong, (objURI + objType).hashCode.toLong, pred)
      }
  }

  def extractDBPediaVertexURI(uri: String): String = {
    uri match {
      case dbpediaUriRegex(_, resourceIdentifier) => resourceIdentifier
      case _ => "unknownResource"
    }
  }

  def extractDBPediaType(uri: String): String = {
    uri match {
      case dbpediaUriRegex(resourceType, _) => resourceType
      case _ => "unknownType"
    }
  }
}
