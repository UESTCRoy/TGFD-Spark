package tgfdDiscovery.script.dbpedia

import org.apache.jena.rdf.model.{ModelFactory, Statement}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData

import java.io.{File, FileOutputStream, PrintWriter}
import scala.collection.mutable
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaIteratorConverter}

object CustomDBPediaFromRDF {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
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

    val sc = spark.sparkContext

    def loadRDF(year: String, fileType: String): RDD[Statement] = {
      val model = ModelFactory.createDefaultModel()
      RDFDataMgr.read(model, s"$baseDir/$year$fileType.ttl")
      sc.parallelize(model.listStatements().toList.asScala)
    }

    years.foreach { year =>
      // Load and process RDF data
      val typesData = loadRDF(year, "types")
      val literalsData = loadRDF(year, "literals")
      val objectsData = loadRDF(year, "objects")

      // Create vertices with types
      val verticesRDD: RDD[(Long, VertexData)] = typesData
        .filter(_.getPredicate.getLocalName.equalsIgnoreCase("type"))
        .map(stmt => {
          val uri = stmt.getSubject.getURI.toLowerCase
          val vertexType = stmt.getObject.asResource.getLocalName.toLowerCase
          val attributes = new mutable.HashMap[String, String]()
          (uri.hashCode.toLong, VertexData(uri, vertexType, attributes))
        })

      // Aggregate attributes into vertices
      val attributesRDD = literalsData
        .filter(_.getObject.isLiteral)
        .map(stmt => {
          val uri = stmt.getSubject.getURI.toLowerCase
          val prop = stmt.getPredicate.getLocalName.toLowerCase
          val value = stmt.getObject.toString
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

      // Create edges from object relationships
      val edgesRDD: RDD[Edge[String]] = objectsData
        .filter(stmt => !stmt.getObject.isLiteral && stmt.getObject.isURIResource)
        .map(stmt => {
          val srcUri = stmt.getSubject.getURI.toLowerCase.hashCode.toLong
          val dstUri = stmt.getObject.asResource.getURI.toLowerCase.hashCode.toLong
          val predicate = stmt.getPredicate.getLocalName.toLowerCase
          Edge(srcUri, dstUri, predicate)
        })

      // Construct the graph
      val graph = Graph(finalVerticesRDD, edgesRDD)

      // Save the combined RDF model
      val outputPath = s"$baseDir/processed/$year-combined.ttl"

//      RDFDataMgr.write(new FileOutputStream(outputPath), combinedModel, RDFFormat.TTL)
    }

    spark.stop()
  }
}
