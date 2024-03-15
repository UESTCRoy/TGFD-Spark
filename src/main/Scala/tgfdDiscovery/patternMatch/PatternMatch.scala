package tgfdDiscovery.patternMatch

import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.graphframes.GraphFrame
import tgfdDiscovery.common.VertexData

object PatternMatch {
  def findMatches(spark: SparkSession, graph: Graph[VertexData, String]): Unit = {
    // 导入隐式转换
    import spark.implicits._

    val getYearOf = udf((attributes: Map[String, String]) => attributes.get("year_of").getOrElse(""))
    val getRatingOf = udf((attributes: Map[String, String]) => attributes.get("rating_of").getOrElse(""))
    val getVotesOf = udf((attributes: Map[String, String]) => attributes.get("votes_of").getOrElse(""))
    val getLanguageOf = udf((attributes: Map[String, String]) => attributes.get("language_of").getOrElse(""))
    val getName = udf((attributes: Map[String, String]) => attributes.get("name").getOrElse(""))
    val getDistributorOf = udf((attributes: Map[String, String]) => attributes.get("distributor_of").getOrElse(""))
    val getEpisodeOf = udf((attributes: Map[String, String]) => attributes.get("episode_of").getOrElse(""))

    // 将VertexRDD[VertexId, VertexData]转换为DataFrame
    val verticesDF = graph.vertices.map { case (id, vertexData) =>
      (id, vertexData.uri, vertexData.vertexType, vertexData.attributes)
    }.toDF("id", "uri", "vertexType", "attributes")

    // 将EdgeRDD[String]转换为DataFrame
    val edgesDF = graph.edges.map(edge => (edge.srcId, edge.dstId, edge.attr)).toDF("src", "dst", "relationship")

    // 创建GraphFrame
    val g = GraphFrame(verticesDF, edgesDF)

    // 定义模式
    val pattern = "(actor)-[e1]->(movie); (actress)-[e2]->(movie); (director)-[e3]->(movie)"

    // 执行模式匹配
    val matches = g.find(pattern)
      .filter($"actor.vertexType" === "actor")
      .filter($"actress.vertexType" === "actress")
      .filter($"movie.vertexType" === "movie")
      .filter($"director.vertexType" === "director")
      .filter($"e1.relationship" === "actor_of")
      .filter($"e2.relationship" === "actress_of")
      .filter($"e3.relationship" === "actor_of")

    val newMatches = matches
      .withColumn("actor_name", getName($"actor.attributes"))
      .withColumn("movie_name", getName($"movie.attributes"))
      .withColumn("movie_rating_of", getRatingOf($"movie.attributes"))
      .withColumn("movie_year_of", getYearOf($"movie.attributes"))
      .withColumn("actress_name", getName($"actress.attributes"))
      .select("actor_name", "actress_name", "movie_name", "movie_rating_of", "movie_year_of")

    newMatches.groupBy("actor_name", "actress_name").count()

    // 展示匹配结果
    newMatches.show(false)
    val matchesCount = matches.count()
    println(s"Found $matchesCount matches.")
  }
}
