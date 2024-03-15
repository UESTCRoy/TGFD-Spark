//package tgfdDiscovery.script
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.log4j.Logger
//import org.apache.spark.graphx._
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import tgfdDiscovery.common.VertexData
//
//import scala.collection.mutable
//import scala.util.matching.Regex
//
//object finalLoader {
//  val logger = Logger.getLogger(this.getClass.getName)
//
//  // 正则表达式定义
//  val uriRegex: Regex = "<[^>]+>".r
//  val literalRegex: Regex = "\"[^\"]+\"".r
//  val typeRegex: Regex = ".*/([^/]+)/[^/]*>$".r
//  val attributeRegex: Regex = "/([^/>]+)>$".r
//
//  def main(args: Array[String]): Unit = {
//    if (args.length < 2) {
//      logger.error("Usage: IMDBLoader <input_directory> <output_directory>")
//      System.exit(1)
//    }
//
//    val spark = SparkSession
//      .builder
//      .appName("RDF Graph Loader")
//      .getOrCreate()
//
//    spark.sparkContext.setLogLevel("WARN")
//
//    val inputDir = args(0)
//    val outputDir = args(1)
//
//    val conf = new Configuration()
//    val fs = FileSystem.get(new java.net.URI(inputDir), conf)
//    val fileStatusList = fs.listStatus(new Path(inputDir))
//
//    fileStatusList.foreach { fileStatus =>
//      val filePath = fileStatus.getPath.toString
//      val fileName = fileStatus.getPath.getName
//      val fileContent = spark.sparkContext.textFile(filePath)
//      println(s"Processing file: $filePath")
//
//      val desiredTypes = Set("movie", "actor", "director", "country", "actress", "genre")
//
//      // 解析文件内容
//      val parsedTriplets = fileContent.flatMap { line =>
//        Option(line).map(_.trim.dropRight(1).replaceAll("""\^\^<http://www\.w3\.org/2001/XMLSchema#(float|integer)>""", "")).flatMap { trimmedLine =>
//          val uriMatches = uriRegex.findAllIn(trimmedLine).toList
//          val literalMatches = literalRegex.findAllIn(trimmedLine).toList
//
//          (uriMatches, literalMatches) match {
//            case (List(subject, predicate), List(literal)) if isDesiredType(subject, desiredTypes) =>
//              val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
//              Some((subject, attributeName, literal.replaceAll("\"", ""), "attribute"))
//            case (List(subject, predicate, obj), _) if isDesiredType(subject, desiredTypes) && isDesiredType(obj, desiredTypes) =>
//              val attributeName = attributeRegex.findFirstMatchIn(predicate).map(_.group(1)).getOrElse("unknown")
//              Some((subject, attributeName, obj, "edge"))
//            case _ => None
//          }
//        }
//      }
//
//      val vertices: RDD[(VertexId, VertexData)] = createVertices(parsedTriplets)
//      val edges: RDD[Edge[String]] = createEdges(parsedTriplets)
//      val graph = Graph(vertices, edges)
//
//      val inDegrees: RDD[(VertexId, Int)] = graph.inDegrees
//      val outDegrees: RDD[(VertexId, Int)] = graph.outDegrees
//
//      val degrees: RDD[(VertexId, (Int, Int))] = inDegrees.fullOuterJoin(outDegrees)
//        .mapValues {
//          case (inOpt, outOpt) => (inOpt.getOrElse(0), outOpt.getOrElse(0))
//        }
//
//      val nonIsolatedVertices: RDD[(VertexId, (VertexData, (Int, Int)))] = graph.vertices
//        .join(degrees)
//        .filter { case (_, (_, (inDeg, outDeg))) => inDeg > 1 || outDeg > 1 }
//
//      val validVertexIds: Set[VertexId] = nonIsolatedVertices.keys.collect().toSet
//      val validGraph = graph.subgraph(vpred = (id, _) => validVertexIds.contains(id))
//
//      println(s"Number of vertices: ${vertices.count()}, Number of edges: ${edges.count()}")
//      println(s"Number of vertices: ${graph.vertices.count()}, Number of edges: ${graph.edges.count()}")
//      println(s"Number of Valid vertices: ${validGraph.vertices.count()}, Number of Valid edges: ${validGraph.edges.count()}")
//
//      val outputFilePath = new Path(outputDir, fileName).toString
//
//      // 转换顶点数据为字符串
//      val vertexLines = validGraph.vertices.map { case (_, vertexData) =>
//        if (vertexData != null && vertexData.vertexType != null && vertexData.uri != null) {
//          val uri = s"<http://imdb.org/${vertexData.vertexType}/${vertexData.uri}>"
//          vertexData.attributes.toSeq.flatMap { case (attrName, attrValue) =>
//            if (attrName != null && attrValue != null) {
//              Some(s"$uri <http://xmlns.com/foaf/0.1/$attrName> \"$attrValue\" .")
//            } else None
//          }.mkString("\n")
//        } else ""
//      }.filter(_.nonEmpty)
//
//      // 创建映射边到它们的源顶点和目标顶点
//      val vertexRdd = validGraph.vertices
//
//      // 将边与其源顶点和目标顶点的数据进行连接
//      val edgesWithVertices = validGraph.edges
//        .map(e => (e.srcId, e))
//        .join(vertexRdd)
//        .map { case (_, (edge, srcVertexData)) => (edge.dstId, (edge, srcVertexData)) }
//        .join(vertexRdd)
//        .map { case (_, ((edge, srcVertexData), dstVertexData)) => (edge, srcVertexData, dstVertexData) }
//
//      val edgeLines = edgesWithVertices.flatMap { case (edge, srcVertexData, dstVertexData) =>
//        if (srcVertexData != null && srcVertexData.vertexType != null && srcVertexData.uri != null &&
//          dstVertexData != null && dstVertexData.vertexType != null && dstVertexData.uri != null) {
//          val srcUri = s"<http://imdb.org/${srcVertexData.vertexType}/${srcVertexData.uri}>"
//          val dstUri = s"<http://imdb.org/${dstVertexData.vertexType}/${dstVertexData.uri}>"
//          Some(s"$srcUri <http://xmlns.com/foaf/0.1/${edge.attr}> $dstUri .")
//        } else {
//          None
//        }
//      }
//
//      // 计算顶点和边的数量
//      val vertexCount = vertexLines.count()
//      val edgeCount = edgeLines.count()
//
//      println(s"Number of vertices: $vertexCount")
//      println(s"Number of edges: $edgeCount")
//
//      // 合并顶点和边的数据
//      val graphData = vertexLines.union(edgeLines)
//
//      // 写入到HDFS
//      graphData.saveAsTextFile(outputFilePath)
//    }
//    println("FINISHED!!!")
//    spark.stop()
//  }
//
//  def createVertices(parsedTriplets: RDD[(String, String, String, String)]): RDD[(VertexId, VertexData)] = {
//    parsedTriplets
//      .filter(_._4 == "attribute")
//      .map { case (subj, attrName, attrValue, _) =>
//        val subURI = extractVertexURI(subj)
//        val vertexType = extractType(subj)
//        ((subURI, vertexType), mutable.HashMap(attrName -> attrValue))
//      }
//      .reduceByKey { (attrs1, attrs2) =>
//        attrs1 ++= attrs2
//      }
//      .map { case ((uri, vertexType), attributes) =>
////        val id = (uri + vertexType).hashCode.toLong
//        (uri.hashCode.toLong, VertexData(uri = uri, vertexType = vertexType, attributes = attributes))
//      }
//  }
//
//  def createEdges(parsedTriplets: RDD[(String, String, String, String)]): RDD[Edge[String]] = {
//    parsedTriplets
//      .filter(_._4 == "edge")
//      .map { case (subj, pred, obj, _) =>
//        val subURI = extractVertexURI(subj)
//        val objURI = extractVertexURI(obj)
//        Edge(subURI.hashCode.toLong, objURI.hashCode.toLong, pred)
//      }
//  }
//
//  def extractVertexURI(uri: String): String = {
//    val trimmedUri = uri.drop(1).dropRight(1)
//    trimmedUri.split("/").last
//  }
//
//  def extractType(uri: String): String = {
//    typeRegex.findFirstMatchIn(uri).map(_.group(1)).getOrElse("unknownType")
//  }
//
//  def isDesiredType(uri: String, desiredTypes: Set[String]): Boolean = {
//    desiredTypes.exists(uri.contains)
//  }
//}
