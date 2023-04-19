object FindPotentialMain {
  def main(args: Array[String]): Unit = {
    val predictionDF = FindPotential.findPotentialDF()
    println(FindPotential.findPotentialByShortName("L. Messi", predictionDF))
    println(FindPotential.findPotentialByPlayerId(158023, predictionDF))
  }
}
