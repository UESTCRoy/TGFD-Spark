package tgfdDiscovery.script.dbpedia

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.jena.rdf.model.{Model, ModelFactory, RDFNode}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.io.FileOutputStream

object LocalDBPediaReader {
  val logger = Logger.getLogger(this.getClass.getName)

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("RDF Graph Loader")
      .master("local[*]")
      .getOrCreate()

    val inputDir = ""

    // 创建一个 Jena 模型
    val model = ModelFactory.createDefaultModel()

    // RDF 数据路径
    val inputPath = "/Users/roy/Desktop/TGFD/datasets/dbpedia/dbpedia-1000/2014/2014-1000.ttl"

    // 使用 Jena 读取 RDF 数据
    RDFDataMgr.read(model, inputPath)

    // Process the RDF data to create a graph
    val graph = DBPediaGraphUtils.createGraph(model, spark)

    // Example action on the graph
    graph.vertices.collect.foreach { case (id, data) =>
      println(s"Vertex ID: $id, Data: $data")
    }

    graph.edges.collect.foreach { edge =>
      println(s"Edge: $edge")
    }

    spark.stop()
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
