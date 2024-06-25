package tgfdDiscovery.script

import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import tgfdDiscovery.common.VertexData

import scala.collection.mutable
import scala.util.matching.Regex

object IMDBGraphUtils {
  val uriRegex: Regex = "<[^>]+>".r
  val literalRegex: Regex = "\"[^\"]+\"".r
  val typeRegex: Regex = ".*/([^/]+)/[^/]*>$".r
  val attributeRegex: Regex = "/([^/>]+)>$".r
  val imdbUriRegex: Regex = "<http://imdb.org/([^/]+)/([^>]+)>".r

  def isDesiredType(uri: String, desiredTypes: Set[String]): Boolean = {
    desiredTypes.exists(uri.contains)
  }

  def createIMDBVertices(parsedTriplets: RDD[(String, String, String, String)]): RDD[(VertexId, VertexData)] = {
    parsedTriplets
      .filter(_._4 == "attribute")
      .map { case (subj, attrName, attrValue, _) =>
        val subURI = extractIMDBVertexURI(subj)
        val vertexType = extractIMDBType(subj)
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

  def createIMDBEdges(parsedTriplets: RDD[(String, String, String, String)]): RDD[Edge[String]] = {
    parsedTriplets
      .filter(_._4 == "edge")
      .map { case (subj, pred, obj, _) =>
        val subURI = extractIMDBVertexURI(subj)
        val subType = extractIMDBType(subj)
        val objURI = extractIMDBVertexURI(obj)
        val objType = extractIMDBType(obj)
        Edge((subURI + subType).hashCode.toLong, (objURI + objType).hashCode.toLong, pred)
      }
  }

  def extractIMDBVertexURI(uri: String): String = {
    uri match {
      case imdbUriRegex(_, resourceIdentifier) => resourceIdentifier
      case _ => "unknownResource"
    }
  }

  def extractIMDBType(uri: String): String = {
    uri match {
      case imdbUriRegex(resourceType, _) => resourceType
      case _ => "unknownType"
    }
  }
}
