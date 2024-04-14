package tgfdDiscovery.dependencyGenerator


object DependencyGenerator {
  def generateCandidateDependency(vertexTypes: Set[String], vertexToAttribute: Map[String, List[String]]): Set[Set[String]] = {
    val attributeCombinations = vertexTypes.flatMap(vertexType =>
      vertexToAttribute.get(vertexType).map(attributes => attributes.map(attribute => s"${vertexType}-$attribute"))
    ).flatten

    val combinations = attributeCombinations.subsets(vertexTypes.size).filter { comb =>
      vertexTypes.forall(vType => comb.exists(_.startsWith(vType)))
    }.toSet

    combinations
  }

  def generateLhsRhsCombinations(dependencies: Set[String]): Set[(Set[String], String)] = {
    dependencies.map { rhs =>
      val lhsCandidates = dependencies - rhs
      (lhsCandidates, rhs)
    }
  }

}
