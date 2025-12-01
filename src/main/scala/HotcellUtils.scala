package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int = {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int = {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  
  // YOU NEED TO CHANGE THIS PART
  /**
   * Calculate if two cells are neighbors using Chebyshev distance
   * Two cells are neighbors if the maximum coordinate difference is <= 1
   */
  def areCellsAdjacent(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int): Boolean = {
    val xDiff = Math.abs(x1 - x2)
    val yDiff = Math.abs(y1 - y2)
    val zDiff = Math.abs(z1 - z2)
    
    // Using Chebyshev distance: max(|x1-x2|, |y1-y2|, |z1-z2|) <= 1
    xDiff <= 1 && yDiff <= 1 && zDiff <= 1
  }

  /**
   * Calculate the Getis-Ord G_i* statistic for hotspot analysis
   */
  def computeGetisOrdStatistic(
      numCells: Double, 
      avgPoints: Double, 
      stdDevPoints: Double, 
      neighborWeightSum: Int, 
      neighborCount: Int, 
      weightedPointSum: Double): Double = {
    
    if (stdDevPoints == 0.0) return 0.0
    
    val numerator = weightedPointSum - (avgPoints * neighborWeightSum)
    val denominatorComponent = (numCells * neighborCount - neighborWeightSum * neighborWeightSum) / (numCells - 1.0)
    
    if (denominatorComponent <= 0.0) return 0.0
    
    val denominator = stdDevPoints * Math.sqrt(denominatorComponent)
    
    if (denominator == 0.0) 0.0 else numerator / denominator
  }

  /**
   * Calculate the squared distance between two cells (used for alternative neighborhood definitions)
   */
  def calculateSpatialTemporalDistance(x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int): Double = {
    val dx = (x1 - x2).toDouble
    val dy = (y1 - y2).toDouble
    val dz = (z1 - z2).toDouble
    dx * dx + dy * dy + dz * dz
  }
  // YOU NEED TO CHANGE THIS PART
}