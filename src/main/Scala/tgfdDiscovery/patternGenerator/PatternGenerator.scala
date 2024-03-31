package tgfdDiscovery.patternGenerator

import tgfdDiscovery.common.{Pattern, PatternTree}

import scala.collection.mutable

object PatternGenerator {

  def generatePatternTrees(vertexTypes: Array[String], frequentEdges: Array[String], k: Int): PatternTree = {
    val tree = new PatternTree(List())

    // 初始化模式树的第一层，每个顶点类型作为一个单独的模式
    val initialPatterns = vertexTypes.map(vt => Pattern(Set(vt), Set())).toList
    tree.addLevel(initialPatterns)

    // 使用广度优先搜索来扩展模式树
    // TODO: 集群模式下可能得把变量改为广播变量
    var currentLevel = initialPatterns
    var edgeCount = 0

    while (edgeCount < k && frequentEdges.nonEmpty && currentLevel.nonEmpty) {
      val nextLevel = List.newBuilder[Pattern]
      val nextLevelVertexTypes = mutable.Set.empty[String]

      // 对当前层的每个模式，尝试添加新的边和顶点
      currentLevel.foreach { pattern =>
        frequentEdges.foreach { edge =>
          // 解析边的信息
          val parts = edge.split("-")
          val srcVertexType = parts(0)
          val edgeLabel = parts(1)
          val dstVertexType = parts(2)

          // 如果模式包含边的起始顶点类型
          if ((pattern.vertices.contains(srcVertexType) && !pattern.vertices.contains(dstVertexType)) ||
            (!pattern.vertices.contains(srcVertexType) && pattern.vertices.contains(dstVertexType))) {
            val newVertices = pattern.vertices + srcVertexType + dstVertexType
            val newEdges = pattern.edges + ((srcVertexType, edgeLabel, dstVertexType))

            if (!nextLevelVertexTypes.contains(srcVertexType) || !nextLevelVertexTypes.contains(dstVertexType)) {
              nextLevelVertexTypes ++= newVertices

              // 创建并添加新的模式
              nextLevel += Pattern(newVertices, newEdges)
            }

          }
        }
      }

      // 准备下一层的迭代
      val newPatterns = nextLevel.result()
      if (newPatterns.nonEmpty) {
        currentLevel = newPatterns
        tree.addLevel(newPatterns)
        edgeCount += 1
      } else {
        currentLevel = List()
      }
    }

    tree
  }
}
