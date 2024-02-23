package tgfdDiscovery.histogram

import org.apache.spark.graphx.{Graph, VertexRDD, EdgeRDD}
import org.apache.spark.rdd.RDD
import tgfdDiscovery.common.VertexData

object Histogram {
  def countVertexTypes(graph: Graph[VertexData, String]): Array[(String, Int)] = {
    graph.vertices.map { case (_, vertexData) =>
      (vertexData.vertexType, 1)
    }.reduceByKey(_ + _).top(10)(Ordering.by(_._2))
  }

  def countEdgeTypes(graph: Graph[VertexData, String]): Array[(String, Int)] = {
    graph.edges.map(edge => (edge.attr, 1)).reduceByKey(_ + _).top(10)(Ordering.by(_._2))
  }

  def countVertexAttributes(graph: Graph[VertexData, String]): Array[(String, Int)] = {
    graph.vertices.flatMap { case (_, vertexData) =>
      vertexData.attributes.keys.map(attribute => (attribute, 1))
    }.reduceByKey(_ + _).top(10)(Ordering.by(_._2))
  }

  def attributeToVertexTypes(graph: Graph[VertexData, String]): RDD[(String, Set[String])] = {
    graph.vertices.flatMap { case (_, vertexData) =>
      vertexData.attributes.keys.map(attribute => (attribute, vertexData.vertexType))
    }
      .distinct() // 去除重复项，确保每个类型只被计数一次
      .aggregateByKey(Set.empty[String])(
        (set, vertexType) => set + vertexType, // 添加顶点类型到集合中
        (set1, set2) => set1 ++ set2 // 合并集合
      )
  }

  def countUniqueCustomEdgeTypes(graph: Graph[VertexData, String]): Array[(String, Int)] = {
    val edgeTypeCombinations = graph.triplets.flatMap { triplet =>
      val srcVertexType = triplet.srcAttr.vertexType
      val dstVertexType = triplet.dstAttr.vertexType
      val edgeType = triplet.attr

      if (srcVertexType != dstVertexType) {
        val key = s"${srcVertexType}-${edgeType}-${dstVertexType}"
        Some((key, 1))
      } else {
        None
      }
    }

    val edgeTypeCounts = edgeTypeCombinations.reduceByKey(_ + _)

    val uniqueEdgeTypeCounts = edgeTypeCounts
      .map{ case (key, count) =>
        val types = key.split("-")
        ((types(0), types(2)), (key, count))
      }
      .reduceByKey((a, b) => if (a._2 > b._2) a else b)
      .map{ case (_, (key, count)) => (key, count) }

    val filteredEdgeTypeCounts = uniqueEdgeTypeCounts.filter(_._2 >= 1000)

    filteredEdgeTypeCounts.takeOrdered(10)(Ordering[Int].reverse.on(_._2))
  }

}
