package tgfdDiscovery.script.dbpedia


import org.apache.jena.rdf.model.{Model, ModelFactory, RDFNode}
import org.apache.jena.riot.{RDFDataMgr, RDFFormat}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import tgfdDiscovery.common.VertexData

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
    val inputPath = "/Users/roy/Desktop/TGFD/datasets/dbpedia/1m_dbpedia/2015/2015-1000000.ttl"

    // 使用 Jena 读取 RDF 数据
    RDFDataMgr.read(model, inputPath)

    // Process the RDF data to create a graph
    val graph = DBPediaGraphUtils.createGraph(model, spark)

    // Example action on the graph
//    graph.vertices.collect.foreach { case (id, data) =>
//      println(s"Vertex ID: $id, Data: $data")
//    }
//
//    graph.edges.collect.foreach { edge =>
//      println(s"Edge: $edge")
//    }

    val typeCounts = countVertexTypes(graph).collect().sortBy(-_._2)

    val maxInDegree = graph.inDegrees.values.max()
    val maxOutDegree = graph.outDegrees.values.max()
    println(s"Max in-degree: $maxInDegree, Max out-degree: $maxOutDegree")

    typeCounts.foreach { case (vertexType, count) =>
      println(s"Type: $vertexType, Count: $count")
    }

    spark.stop()
  }

  def countVertexTypes(graph: Graph[(String, List[String]), String]): RDD[(String, Int)] = {
    graph.vertices.flatMap { case (_, (uri, triples)) =>
        // Map over triples to split them and find the type predicate
        triples.flatMap { triple =>
          // Remove the surrounding brackets and split by comma
          val parts = triple.trim.stripPrefix("[").stripSuffix("]").split(",")
            .map(_.trim)  // Remove any leading/trailing whitespaces from each part

          // Check if the predicate (second element) matches the type URI
          if (parts.length == 3 && parts(1).trim == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type") {
            // Split the object part on '/' and take the last part, which is the type name
            val typeName = parts(2).trim.replace("<", "").replace(">", "").split('/').last
            Some(typeName)
          } else None
        }
      }
      .map((_, 1))  // Prepare for counting by mapping each type to 1
      .reduceByKey(_ + _)  // Reduce by key to count occurrences of each type
  }


//  def countVertexTypes(graph: Graph[(String, List[String]), String]): RDD[(String, Int)] = {
//    val debug1 = graph.vertices.flatMap { case (_, (uri, triples)) =>
//      println(s"Processing URI: $uri with triples: $triples")  // Debug output to check what triples are being processed
//      val typeTriples = triples.filter(triple => triple.contains(",http://www.w3.org/1999/02/22-rdf-syntax-ns#type,"))
//      println(s"Filtered type triples: $typeTriples")  // Check what gets filtered as type triples
//
//      typeTriples.map { triple =>
//        val parts = triple.split(" ")
//        if (parts.length >= 3) Some(parts(2).replace("<", "").replace(">", "").trim)
//        else None
//      }.filter(_.isDefined).map(_.get)
//    }
//
//    println(s"Types extracted: ${debug1.collect().toList}")  // Collect and print to debug
//
//    val typeCounts = debug1.map((_, 1)).reduceByKey(_ + _)
//    typeCounts
//  }


}
