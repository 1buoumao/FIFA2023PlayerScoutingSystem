object FindPotentialMain {
  def main(args: Array[String]): Unit = {
    val predictionDF = FindPotential.findPotentialDF()
//    println(FindPotential.findPotentialByShortName("L. Messi", predictionDF))
//    println(FindPotential.findPotentialByPlayerId(158023, predictionDF))
//    println(FindPotential.findPotentialByPlayerId(114514, predictionDF))
    println(FindPotential.ifNameExists("L. meidong", predictionDF))
    println(FindPotential.ifNameExists("L. Messi", predictionDF))
    println(FindPotential.ifPlayerIdExists(1145141919, predictionDF))
    println(FindPotential.ifPlayerIdExists(158023, predictionDF))
    // usage should be like this.
    if (FindPotential.ifNameExists("L. Messi", predictionDF)){
      println(FindPotential.findPotentialByShortName("L. Messi",predictionDF))
    }
  }
}
