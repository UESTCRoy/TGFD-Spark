package tgfdDiscovery.supportCompute

object SupportCompute {
  def countPairs(list: List[Int]): Int = {
    val positiveCount = list.count(_ > 0)
    positiveCount * (positiveCount - 1) / 2
  }

  def binomialCoefficient(n: Int, k: Int): BigInt = {
    (0 until k).foldLeft(BigInt(1)) { (result, i) =>
      result * (n - i) / (i + 1)
    }
  }

  def calculateTGFDSupport(list: List[Int], dfCount: Long, T: Int): Double = {
    val numerator = countPairs(list)
    val denominator = dfCount * binomialCoefficient(T + 1, 2).toDouble
    if (numerator > denominator) {
      throw new IllegalArgumentException("numerator > denominator")
    }
    numerator.toDouble / denominator
  }

}

