package tgfdDiscovery.script.dbpedia

import org.apache.jena.rdf.model.{ModelFactory, Statement}
import org.apache.jena.riot.RDFDataMgr
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData

import scala.collection.mutable
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaIteratorConverter}

case class SimpleStatement(subject: String, predicate: String, objectStr: String)

object LocalDBPediaTest {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("DBpedia Data Integration")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val baseDir = "/Users/roy/Desktop/TGFD/datasets/dbpedia/test"

    val years = Seq("2014", "2015", "2016", "2017")

    val sc = spark.sparkContext

    // Function to load RDF data and convert to SimpleStatement RDD
    def loadRDF(fileType: String): RDD[SimpleStatement] = {
      val model = ModelFactory.createDefaultModel()
      RDFDataMgr.read(model, s"$baseDir/2015$fileType.ttl")
      val statements = model.listStatements().asScala.map(stmt =>
        SimpleStatement(
          stmt.getSubject.toString,
          stmt.getPredicate.toString,
          stmt.getObject.toString
        )).toList
      sc.parallelize(statements)
    }

    // Load and process RDF data
    val typesData = loadRDF("types")
    val literalsData = loadRDF("literals")
    val objectsData = loadRDF("objects")

    // Create vertices with types
    val verticesRDD: RDD[(Long, VertexData)] = typesData
      .filter(_.predicate.endsWith("#type"))
      .map(stmt => {
        val uri = stmt.subject.toLowerCase
        val vertexType = stmt.objectStr.toLowerCase
        val attributes = new mutable.HashMap[String, String]()
        (uri.hashCode.toLong, VertexData(uri, vertexType, attributes))
      })

    // Aggregate attributes into vertices
    val attributesRDD = literalsData
      .filter(_.predicate.contains("/ontology/"))
      .map(stmt => {
        val uri = stmt.subject.toLowerCase
        val prop = stmt.predicate.toLowerCase
        val value = stmt.objectStr
        (uri.hashCode.toLong, (prop, value))
      })
      .groupByKey()

    // Join attributes to vertices
    val finalVerticesRDD = verticesRDD.leftOuterJoin(attributesRDD).map {
      case (id, (vertexData, Some(attrs))) =>
        attrs.foreach { case (prop, value) => vertexData.attributes += (prop -> value) }
        (id, vertexData)
      case (id, (vertexData, None)) => (id, vertexData)
    }

    val vertexIdsRDD = finalVerticesRDD.map(_._1).distinct()
    val vertexIdsMappedRDD = vertexIdsRDD.map(id => (id, true))

    val potentialEdgesRDD = objectsData
      .filter(stmt => !stmt.objectStr.startsWith("\""))
      .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, (stmt.objectStr.toLowerCase.hashCode.toLong, stmt.predicate.toLowerCase)))

    val validSrcEdgesRDD = potentialEdgesRDD.join(vertexIdsMappedRDD).map {
      case (srcId, ((dstId, pred), _)) => (dstId, (srcId, pred))
    }

    val validEdgesRDD = validSrcEdgesRDD.join(vertexIdsMappedRDD).map {
      case (dstId, ((srcId, pred), _)) => Edge(srcId, dstId, pred)
    }

    val graph = Graph(finalVerticesRDD, validEdgesRDD)

    val vertexCount = graph.vertices.count()
    val edgeCount = graph.edges.count()
    println(s"Graph has $vertexCount vertices and $edgeCount edges")

    // Print a few vertices
    graph.vertices.take(10).foreach { case (vertexId, vertexData) =>
      println(s"Vertex ID: $vertexId with Data: $vertexData")
    }

    // Print a few edges
    graph.edges.take(10).foreach(edge =>
      println(s"Edge: ${edge.srcId} to ${edge.dstId} with Attribute: ${edge.attr}")
    )

    spark.stop()
  }
}
