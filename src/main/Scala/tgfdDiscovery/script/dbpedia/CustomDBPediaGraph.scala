package tgfdDiscovery.script.dbpedia

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.FileOutputStream

object CustomDBPediaGraph {
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
      .appName("Generate Custom DBPedia Graph")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val inputDir = args(0)
    val outputDir = args(1)
    val edgeSize = args(2)

    val conf = new Configuration()
    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
    val fileStatusList = fs.listStatus(new Path(inputDir))

    var previousSelectedEdges: RDD[Edge[String]] = spark.sparkContext.emptyRDD[Edge[String]]

    fileStatusList.foreach { fileStatus =>
      val filePath = fileStatus.getPath.toString
      val fileName = fileStatus.getPath.getName

      val model = ModelFactory.createDefaultModel()
      RDFDataMgr.read(model, filePath)


    }
  }

  def writeGraphToTTL(vertices: RDD[(VertexId, (String, List[String]))], outputPath: String): Unit = {
    val model = ModelFactory.createDefaultModel()

    vertices.collect().foreach { case (_, (uri, properties)) =>
      val subject = model.createResource(uri)
      properties.foreach { property =>
        val parts = property.split(", ") // Split the property string into RDF triple components
        val predicate = model.createProperty(parts(1))
        if (parts(2).startsWith("http")) {
          val obj = model.createResource(parts(2))
          model.add(subject, predicate, obj)
        } else {
          val obj = model.createLiteral(parts(2))
          model.add(subject, predicate, obj)
        }
      }
    }

    RDFDataMgr.write(new FileOutputStream(outputPath), model, RDFFormat.TTL)
  }
}
