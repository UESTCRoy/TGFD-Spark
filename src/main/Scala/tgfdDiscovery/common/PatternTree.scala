package tgfdDiscovery.common

class PatternTree(var levels: List[List[Pattern]]) {
  def addLevel(level: List[Pattern]): Unit = {
    levels = levels :+ level
  }
}