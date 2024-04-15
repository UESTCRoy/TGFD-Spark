package tgfdDiscovery.deltaGenerator

object DeltaGenerator {
  def getMinMaxPair(timestampCounts: List[Int]): Option[(Int, Int)] = {
    val occurIndices = timestampCounts.zipWithIndex.collect {
      case (count, index) if count > 0 => index
    }

    if (occurIndices.length < 2) {
      None
    } else {
      occurIndices match {
        case first :: Nil if timestampCounts(first) > 1 => Some((0, 0))
        case first :: rest =>
          val minDistance = (first :: rest).sliding(2).map {
            case List(a, b) => b - a
          }.min

          val maxDistance = rest.last - first

          if (minDistance <= maxDistance) Some((minDistance, maxDistance)) else None
      }
    }
  }


  def main(args: Array[String]): Unit = {
    //    val timestamps = List(0, 1, 1, 0, 1, 0, 1, 1)
    val timestamps = List(1, 1)
    val result = getMinMaxPair(timestamps)
    println(result)
  }
}
