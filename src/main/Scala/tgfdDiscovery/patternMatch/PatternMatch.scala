package tgfdDiscovery.patternMatch

import org.apache.spark.graphx._
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, collect_list, lit, udf}
import org.apache.spark.sql.types.StringType
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

  def processFramesAndDependency(framesWithIndex: Seq[(DataFrame, Int)], dependency: Set[String]): DataFrame = {
    val mergeFlagsUDF = udf((arrays: Seq[Seq[Int]]) => arrays.transpose.map(_.reduce((a, b) => a | b)))

    val modifiedFrames = framesWithIndex.map { case (frame, index) =>
      val modifiedFrame = PatternMatch.applyDependencyAttributes(frame, dependency)
      val filteredFrame = PatternMatch.filterDataFrame(modifiedFrame)
      val flagArray = Array.fill(framesWithIndex.length)(0)
      flagArray(index) = 1
      filteredFrame.withColumn("presence_flags", array(flagArray.map(lit): _*))
    }

    val combinedDf = modifiedFrames.reduce(_ unionByName _)
    val groupingColumns = combinedDf.columns.filterNot(_ == "presence_flags").map(col)

    combinedDf.groupBy(groupingColumns: _*)
      .agg(collect_list("presence_flags").as("collected_flags"))
      .withColumn("presence_flags", mergeFlagsUDF(col("collected_flags")))
      .drop("collected_flags")
  }

  private def filterDataFrame(df: DataFrame): DataFrame = {
    val stringFields = df.schema.fields.filter(_.dataType == StringType).map(_.name)
    stringFields.foldLeft(df) { (currentDf, fieldName) =>
      currentDf.filter(!(col(fieldName).equalTo("????") || col(fieldName).isNull || col(fieldName).equalTo("")))
    }
  }

  private def generateQueryAndFilters(edges: Seq[(String, String, String)]): (String, Seq[org.apache.spark.sql.Column]) = {
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

  private def applyDependencyAttributes(df: DataFrame, dependencies: Set[String]): DataFrame = {
    var newColumns: Seq[String] = Seq()

    val updatedDf = dependencies.foldLeft(df) { (currentDf, dependency) =>
      val parts = dependency.split("-")
      if (parts.length == 2) {
        val vertexLabel = parts(0)
        val attributeName = parts(1)
        attributeExtractors.get(attributeName) match {
          case Some(udfFunction) =>
            val newColName = s"${vertexLabel}-$attributeName"
            newColumns = newColumns :+ newColName
            currentDf.withColumn(newColName, udfFunction(col(s"$vertexLabel.attributes")))
          case None => currentDf
        }
      } else currentDf
    }

    updatedDf.select(newColumns.head, newColumns.tail: _*)
  }
}
