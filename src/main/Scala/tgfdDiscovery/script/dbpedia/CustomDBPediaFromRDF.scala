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
    if (args.length < 2) {
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

    val years = Seq("2014", "2015", "2016", "2017")

    // Function to load RDF data and convert to SimpleStatement RDD
    def loadRDF(year: String, fileType: String): RDD[SimpleStatement] = {
      val model = ModelFactory.createDefaultModel()
      RDFDataMgr.read(model, s"$baseDir/$year/$year$fileType.ttl")
      spark.sparkContext.parallelize(model.listStatements().asScala.map(stmt =>
        SimpleStatement(stmt.getSubject.toString, stmt.getPredicate.toString, stmt.getObject.toString)).toList)
    }

    def extractLastPart(uri: String): String = uri.substring(uri.lastIndexOf("/") + 1).toLowerCase

    years.foreach { year =>
      // Load and process RDF data
      val typesData = loadRDF(year, "types")
      val literalsData = loadRDF(year, "literals")
      val objectsData = loadRDF(year, "objects")

      val verticesRDD: RDD[(Long, VertexData)] = typesData
        .filter(_.predicate.endsWith("#type"))
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, VertexData(stmt.subject, extractLastPart(stmt.objectStr), new mutable.HashMap())))
        .reduceByKey((data1, data2) => data1)

      val attributesRDD = literalsData
        .filter(_.predicate.contains("/ontology/"))
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, (extractLastPart(stmt.predicate), stmt.objectStr.split("\\^\\^")(0))))
        .groupByKey()

      val finalVerticesRDD = verticesRDD.leftOuterJoin(attributesRDD).map {
        case (id, (vertexData, Some(attrs))) =>
          attrs.foreach { case (prop, value) => vertexData.attributes += (prop -> value) }
          (id, vertexData)
        case (id, (vertexData, None)) => (id, vertexData)
      }

      val filteredEdgesRDD = objectsData
        .filter(stmt => stmt.objectStr.startsWith("http") && !stmt.predicate.toLowerCase.endsWith("seealso"))
        .map(stmt => (stmt.subject.toLowerCase.hashCode.toLong, (stmt.objectStr.toLowerCase.hashCode.toLong, extractLastPart(stmt.predicate))))
        .join(finalVerticesRDD)
        .map { case (srcId, ((dstId, pred), _)) => (dstId, (srcId, pred)) }
        .join(finalVerticesRDD)
        .map { case (dstId, ((srcId, pred), _)) => Edge(srcId, dstId, pred) }

      val graph = Graph(finalVerticesRDD, filteredEdgesRDD)
      println(s"Graph created with ${graph.vertices.count()} vertices and ${graph.edges.count()} edges.")

      // Compute the degrees and create a connected graph
      val degreesRDD = graph.degrees.cache()
      val connectedGraph = graph.outerJoinVertices(degreesRDD) {
        (vid, vd, degreeOpt) => (vd, degreeOpt.getOrElse(0))
      }.subgraph(vpred = (_, attr) => attr._2 > 0).mapVertices((id, attr) => attr._1)
      println(s"Connected graph created with ${connectedGraph.vertices.count()} vertices and ${connectedGraph.edges.count()} edges.")

      // Continue with processing as before
      val vertexLines = connectedGraph.vertices.flatMap {
        case (_, vertexData) =>
          val newURI = s"<http://dbpedia.org/${vertexData.vertexType}/${extractLastPart(vertexData.uri)}>"
          vertexData.attributes.map {
            case (attrName, attrValue) => s"""$newURI <http://xmlns.com/foaf/0.1/$attrName> "$attrValue" ."""
          }
      }

      val edgeLines = connectedGraph.triplets.map { triplet =>
        val srcUri = s"<http://dbpedia.org/${triplet.srcAttr.vertexType}/${extractLastPart(triplet.srcAttr.uri)}>"
        val dstUri = s"<http://dbpedia.org/${triplet.dstAttr.vertexType}/${extractLastPart(triplet.dstAttr.uri)}>"
        s"$srcUri <http://xmlns.com/foaf/0.1/${triplet.attr}> $dstUri ."
      }

      val graphData = vertexLines.union(edgeLines)
      val outputPath = s"$outputDir/$year.nt"
      graphData.saveAsTextFile(outputPath)
    }
    println("Data saved successfully")
    spark.stop()
  }
}
