package tgfdDiscovery.script.dbpedia

import org.apache.jena.rdf.model.{ModelFactory, Statement}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.{SimpleStatement, VertexData}

import java.io.{File, FileOutputStream, PrintWriter}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaIteratorConverter}

object CustomDBPediaFromRDF {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.error("Usage: DBPediaLoader <input_directory> <output_directory>")
      System.exit(1)
    }

    val spark = SparkSession
      .builder
      .appName("DBpedia Data Integration")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val baseDir = args(0)
    val outputDir = args(1)
    val edgeSize = args(2)

    val years = Seq("2014", "2015", "2016", "2017")

    // Function to load RDF data and convert to SimpleStatement RDD
    def loadRDF(year: String, fileType: String): RDD[SimpleStatement] = {
      val model = ModelFactory.createDefaultModel()
      RDFDataMgr.read(model, s"$baseDir/$year$fileType.ttl")
      spark.sparkContext.parallelize(model.listStatements().asScala.map(stmt =>
        SimpleStatement(stmt.getSubject.toString, stmt.getPredicate.toString, stmt.getObject.toString)).toList)
    }

    def extractLastPart(uri: String): String = uri.substring(uri.lastIndexOf("/") + 1).toLowerCase

    years.foreach { year =>
      // Load and process RDF data
      val typesData = loadRDF(year, "types")
      val literalsData = loadRDF(year, "literals")
      val objectsData = loadRDF(year, "objects")

      // Create vertices with types
      val verticesRDD: RDD[(Long, VertexData)] = typesData
        .filter(_.predicate.endsWith("#type"))
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, VertexData(stmt.subject, extractLastPart(stmt.objectStr), new mutable.HashMap())))
        .reduceByKey((data1, data2) => data1) // Remove duplicates

      // Aggregate attributes into vertices
      val attributesRDD = literalsData
        .filter(_.predicate.contains("/ontology/"))
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, (extractLastPart(stmt.predicate), stmt.objectStr.split("\\^\\^")(0))))
        .groupByKey()

      // Join attributes to vertices
      val finalVerticesRDD = verticesRDD.leftOuterJoin(attributesRDD).map {
        case (id, (vertexData, Some(attrs))) =>
          attrs.foreach { case (prop, value) => vertexData.attributes += (prop -> value) }
          (id, vertexData)
        case (id, (vertexData, None)) => (id, vertexData)
      }

      val validEdgesRDD = objectsData
        .filter(_.objectStr.startsWith("http"))  // Ensuring that object URI is valid
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, (stmt.objectStr.toLowerCase.hashCode.toLong, extractLastPart(stmt.predicate))))
        .join(finalVerticesRDD) // Join with source vertex data
        .map { case (srcId, ((dstId, pred), srcVertexData)) => (dstId, (srcId, pred)) }
        .join(finalVerticesRDD) // Join with destination vertex data
        .map { case (dstId, ((srcId, pred), dstVertexData)) =>
          Edge(srcId, dstId, pred)
        }

      val graph = Graph(finalVerticesRDD, validEdgesRDD)
      println(s"Number of vertices: ${graph.vertices.count()}, Number of edges: ${graph.edges.count()}")

      val vertexLines = finalVerticesRDD.flatMap { case (_, vertexData) =>
        val newURI = s"<http://dbpedia.org/${vertexData.vertexType}/${extractLastPart(vertexData.uri)}>"
        vertexData.attributes.toSeq.map {
          case (attrName, attrValue) =>
            s"""$newURI <http://xmlns.com/foaf/0.1/$attrName> "$attrValue" ."""
        }
      }

      val edgeLines = objectsData
        .filter(_.objectStr.startsWith("http"))
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, (stmt.objectStr.toLowerCase.hashCode.toLong, stmt.predicate.toLowerCase)))
        .join(finalVerticesRDD)
        .map { case (srcId, ((dstId, pred), srcVertexData)) => (dstId, (srcId, pred, srcVertexData)) }
        .join(finalVerticesRDD)
        .map { case (dstId, ((srcId, pred, srcVertexData), dstVertexData)) =>
          val srcUri = s"<http://dbpedia.org/${extractLastPart(srcVertexData.vertexType)}/${extractLastPart(srcVertexData.uri)}>"
          val dstUri = s"<http://dbpedia.org/${extractLastPart(dstVertexData.vertexType)}/${extractLastPart(dstVertexData.uri)}>"
          s"$srcUri <http://xmlns.com/foaf/0.1/${extractLastPart(pred)}> $dstUri ."
        }

      // Save the combined RDF model
      val outputPath = s"$baseDir/processed/$year-$edgeSize.ttl"

    }

    spark.stop()
  }
}
