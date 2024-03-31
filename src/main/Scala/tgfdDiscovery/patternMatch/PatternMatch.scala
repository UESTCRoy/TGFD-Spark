package tgfdDiscovery.patternMatch

import org.apache.spark.graphx._
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, udf}
import org.graphframes.GraphFrame
import tgfdDiscovery.common.VertexData

object PatternMatch {

  val getYearOf = udf((attributes: Map[String, String]) => attributes.get("year_of").getOrElse(""))
  val getRatingOf = udf((attributes: Map[String, String]) => attributes.get("rating_of").getOrElse(""))
  val getVotesOf = udf((attributes: Map[String, String]) => attributes.get("votes_of").getOrElse(""))
  val getLanguageOf = udf((attributes: Map[String, String]) => attributes.get("language_of").getOrElse(""))
  val getName = udf((attributes: Map[String, String]) => attributes.get("name").getOrElse(""))
  val getDistributorOf = udf((attributes: Map[String, String]) => attributes.get("distributor_of").getOrElse(""))
  val getEpisodeOf = udf((attributes: Map[String, String]) => attributes.get("episode_of").getOrElse(""))

  val attributeExtractors = Map(
    "year_of" -> getYearOf,
    "rating_of" -> getRatingOf,
    "votes_of" -> getVotesOf,
    "language_of" -> getLanguageOf,
    "name" -> getName,
    "distributor_of" -> getDistributorOf,
    "episode_of" -> getEpisodeOf
  )

  def findMatches(spark: SparkSession, graph: Graph[VertexData, String], edges: Set[(String, String, String)]): sql.DataFrame = {
    import spark.implicits._

    val verticesDF = graph.vertices.map { case (id, vertexData) =>
      (id, vertexData.uri, vertexData.vertexType, vertexData.attributes)
    }.toDF("id", "uri", "vertexType", "attributes")

    val edgesDF = graph.edges.map(edge => (edge.srcId, edge.dstId, edge.attr)).toDF("src", "dst", "relationship")

    val g = GraphFrame(verticesDF, edgesDF)

    val (queryPattern, filters) = generateQueryAndFilters(edges.toSeq)
    val matches = g.find(queryPattern)

    val filteredMatches = filters.foldLeft(matches) { (df, condition) => df.filter(condition) }
//    filteredMatches.show(false)

//    val matchesCount = filteredMatches.count()
//    println(s"Found $matchesCount matches.")

    filteredMatches
  }

  def generateQueryAndFilters(edges: Seq[(String, String, String)]): (String, Seq[org.apache.spark.sql.Column]) = {
    val pattern = edges.zipWithIndex.map { case ((src, rel, dst), index) =>
      s"($src)-[e$index]->($dst)"
    }.mkString("; ")

    val filters = edges.zipWithIndex.flatMap { case ((src, rel, dst), index) =>
      Seq(
        col(s"${src}.vertexType") === lit(src),
        col(s"e$index.relationship") === lit(rel),
        col(s"${dst}.vertexType") === lit(dst)
      )
    }

    (pattern, filters)
  }

  def applyDependencyAttributes(df: DataFrame, dependencies: Seq[(Set[String], String)]): DataFrame = {
    dependencies.foldLeft(df) { case (currentDf, (dependencyLhs, dependencyRhs)) =>
      val updatedDf = dependencyLhs.foldLeft(currentDf) { (df, lhs) =>
        val lhsParts = lhs.split("\\.")
        if (lhsParts.length == 2) {
          val vertexLabel = lhsParts(0)
          val attributeName = lhsParts(1)
          attributeExtractors.get(attributeName) match {
            case Some(udfFunction) => df.withColumn(s"${vertexLabel}_${attributeName}", udfFunction(col(s"$vertexLabel.attributes")))
            case None => df
          }
        } else df
      }

      val rhsParts = dependencyRhs.split("\\.")
      if (rhsParts.length == 2) {
        val vertexLabel = rhsParts(0)
        val attributeName = rhsParts(1)
        attributeExtractors.get(attributeName) match {
          case Some(udfFunction) => updatedDf.withColumn(s"${vertexLabel}_${attributeName}", udfFunction(col(s"$vertexLabel.attributes")))
          case None => updatedDf
        }
      } else updatedDf
    }
  }
}
