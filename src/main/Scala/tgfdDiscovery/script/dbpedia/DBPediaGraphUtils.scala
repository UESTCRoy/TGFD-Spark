package tgfdDiscovery.script.dbpedia

import org.apache.jena.rdf.model.Model
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData

import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter

// Two Version:
// 1. VertexData -> (String, List[String]) for script
// 2. VertexData -> VertexData for spark mining
object DBPediaGraphUtils {
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

    //    val vertices: RDD[(VertexId, VertexData)] = spark.sparkContext.parallelize(
    //      model.listSubjects().asScala.filter(_.isURIResource).map { resource =>
    //        val uri = resource.getURI
    //        val id = uri.hashCode.toLong
    //
    //        // Determine vertex type (adjust RDF property as needed)
    //        val vertexType = model.listStatements(resource, model.getProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), null)
    //          .asScala.toSeq.headOption
    //          .map(_.getObject.asResource.getURI)
    //          .getOrElse("Unknown")
    //
    //        // Collect attributes
    //        val attributes = mutable.HashMap[String, String]()
    //        model.listStatements(resource, null, null).asScala.foreach { stmt =>
    //          if (!stmt.getPredicate.getURI.endsWith("#type")) {
    //            val objectValue = if (stmt.getObject.isLiteral) {
    //              stmt.getObject.asLiteral.getString
    //            } else {
    //              stmt.getObject.toString
    //            }
    //            attributes(stmt.getPredicate.getLocalName) = objectValue
    //          }
    //        }
    //
    //        (id, VertexData(uri, vertexType, attributes))
    //      }.toList
    //    )

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
}
