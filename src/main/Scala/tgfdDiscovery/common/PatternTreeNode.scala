package tgfdDiscovery.common

class PatternTreeNode(val vertex: Vertex, val edges: List[RelationshipEdge]) {
  var children: List[PatternTreeNode] = List()

  def addChild(node: PatternTreeNode): Unit = {
    children = children :+ node
  }
}
