package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
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

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def is_neighbor(x1: Int, y1: Int, z1: Int,
                  x2: Int, y2: Int, z2: Int): Boolean = {
    if (math.abs(x1 - x2) <= 1
      && math.abs(y1 - y2) <= 1
      && math.abs(z1 - z2) <= 1) {
      return true
    } else {
      return false
    }
  }

  def getis_ord(numCells: Double, mean: Double, std: Double, sum_neigh: Int,x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Double = {
    var adj = 27
    if (x == minX || x == maxX){
      if ((y == minY || y == maxY) && (z == minZ || z == maxZ)){
        adj = 8
      }
      else if ((y == minY || y == maxY) || (z == minZ || z == maxZ)){
        adj = 12
      }
      else {
        adj = 18
      }
    }
    else if(y == minY || y == maxY){
      if((z == minZ || z == maxZ)){
        adj = 12
      }
      else{
        adj = 18
      }
    }
    else if (z == minZ || z == maxZ){
      adj = 18
    }

    val top = (sum_neigh.toDouble - mean * adj)
    val bottom = (std * math.sqrt((numCells * adj - (adj * adj)) / (numCells - 1.0)))
    if (bottom == 0.0){return 0.0}
    return (top/bottom)
  }
}
