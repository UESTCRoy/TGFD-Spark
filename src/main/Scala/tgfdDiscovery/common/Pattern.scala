package tgfdDiscovery.common

case class Pattern(vertices: Set[String], edges: Set[(String, String, String)])  // (srcVertexType, edgeType, dstVertexType)