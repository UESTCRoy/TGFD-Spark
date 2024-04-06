package tgfdDiscovery.dependencyGenerator

import scala.collection.mutable.ListBuffer

object DependencyGenerator {

  def generateCombinations(vertexToAttribute: Map[String, List[String]]): List[(Set[String], String)] = {
    val vertexTypes = vertexToAttribute.keySet.toList

    vertexTypes.flatMap { vertexType =>
      val leftSideSet = vertexTypes.filter(_ != vertexType).toSet
      val combinations = generateAttributeCombinations(vertexToAttribute, leftSideSet.toList)
      val targetAttributes = vertexToAttribute(vertexType)

      for {
        combination <- combinations
        targetAttribute <- targetAttributes
      } yield (combination.toSet, s"$vertexType.$targetAttribute")
    }
  }

  private def generateAttributeCombinations(vertexToAttribute: Map[String, List[String]], leftSideSet: List[String]): List[List[String]] = {
    var attributeCombinations = List[List[String]]()

    leftSideSet.foreach { typeKey =>
      val attributes = vertexToAttribute(typeKey)
      attributeCombinations = cartesianProduct(attributeCombinations, attributes, typeKey)
    }

    attributeCombinations
  }

  private def cartesianProduct(currentCombinations: List[List[String]], newAttributes: List[String], typeKey: String): List[List[String]] = {
    var newCombinations = ListBuffer[List[String]]()

    if (currentCombinations.isEmpty) {
      newAttributes.foreach { attr =>
        newCombinations += List(s"$typeKey.$attr")
      }
    } else {
      currentCombinations.foreach { currentCombination =>
        newAttributes.foreach { attr =>
          newCombinations += (currentCombination :+ s"$typeKey.$attr")
        }
      }
    }

    newCombinations.toList
  }
}
