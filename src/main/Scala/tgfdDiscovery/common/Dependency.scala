package tgfdDiscovery.common

case class Dependency(lhsAttributes: Set[String], rhsAttribute: String, lhsValues: Set[String], rhsValue: String)