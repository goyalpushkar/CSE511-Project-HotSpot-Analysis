package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01
  val timeStep = 1

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

  // YOU NEED TO CHANGE THIS PART
   def is_neighbor(x1: Double, y1: Double, z1: Double,
                   x2: Double, y2: Double, z2: Double): Boolean = {
    if (math.abs(x1 - x2) <= 1 // coordinateStep
      && math.abs(y1 - y2) <= 1 //coordinateStep
      && math.abs(z1 - z2) <= 1 //timeStep
      ) {
      return true
    } else {
      return false
    }
  }

  //As per problem statement - http://sigspatial2016.sigspatial.org/giscup2016/problem
  //Spatial neighborhood of each cell is established by subdividing longitude and latitude uniformly.
  //The time axis is subdivided into preceding, current and following time periods (3 sub cubes)
   
  //Each cube of neighborhoods is sub-divided into 27 sub-cubes (9 in front face, 9 in back face, 3 (middle row) on the right face, 3 (middle row) on left face and 1 each on top and bottom and 1 in middle (center))
  //The cubes which are in the center of each neighborhood cube will have 26 neighbors
   
  //This way the cubes which are on the edges and corners of the boundary of the problem statement will not have complete 26 neighbors
  //There are total 9 + 9 + 3 + 3 + 1 + 1 = 26 which have atleast one visible face.
  //There are total 8 cubes which are on corners (3 visible faces/Axis matching with boundary)
  //There are total 12 cubes which are on the edges  (2 visible faces/Axis matching with boundary)
  //There are total 26 - 8 - 12 = 6 (1 visible face)
  def get_num_of_neighbours(x: Double, y: Double, z: Double
                           ,minX: Double, maxX: Double
                           ,minY: Double, maxY: Double
                           ,minZ: Double, maxZ: Double): Int = {    
    var neighbour = 27   //26
    if ( x == minX || x == maxX ){  // X axis is matching with the boundary
      if ( ( y == minY || y == maxY ) && (z == minZ || z == maxZ) ){   //All 3 axis are matching with boundary
        neighbour = 8
      }
      else if ( (y == minY || y == maxY) || (z == minZ || z == maxZ) ){  //2 of the axis are matching with boundary
        neighbour = 12
      }
      else    // Not at the corners of the boundary as Y and Z are not matching i.e. 26(total neighbors) - 8(corner cubes)
        neighbour = 18  //6   
    }  
    else if (y == minY || y == maxY){ // Y axis is matching with the boundary and X axis is not at boundary
      if (z == minZ || z == maxZ){   //Y and Z - 2 axis are matching with boundary
        neighbour = 12
      }else
        neighbour = 18 //6    // Not at the corners of the boundary as X and Z both are not matching i.e. 26(total neighbors) - 8(corner cubes)
    }
    else if (z == minZ || z == maxZ){  // Z axis is matching with the boundary and X-Y are not at the boundary
      neighbour = 18  //6     // Not at the corners of the boundary as X and Y both are not matching i.e. 26(total neighbors) - 8(corner cubes)
    }
    return neighbour    //If none of the boundary axis are matching. i.e cube is inside the boundary and have default neighbors.
  }
  
  def getis_ord_cal(spatial_neighbor_weight: Double, noOfNeighbors: Int, mean: scala.math.BigDecimal, std_deviation: scala.math.BigDecimal, numCells: Double): BigDecimal = {
     val numerator = ( spatial_neighbor_weight - ( mean * noOfNeighbors ) )
     val denominator = (   ( std_deviation ) 
                          * math.sqrt( ( ( numCells * noOfNeighbors ) - math.pow(noOfNeighbors,2) ) 
                                     / ( numCells - 1 ) 
                                     ) 
                        ) 
     if (denominator == 0 ){
       return 0.0
     }else{
       return numerator/denominator
     }
  }
  
  def getis_ord(spatial_neighbor_weight: Double, mean: scala.math.BigDecimal, std_deviation: scala.math.BigDecimal): BigDecimal = {
     //As per Live Event wi,j i.e. spatial weight between cell i and j is equal and assumed to be 1
     val numerator = spatial_neighbor_weight - mean
     val denominator = std_deviation
     if (denominator == 0 ){
       return 0.0
     }else{
       return numerator/denominator
     }
  }
  
  /*def getis_ord(numCells: Double, mean: Double, std: Double, sum_neigh: Int,x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Double = {
  
    //Candice
    //val g_score = (sum_neigh - mean * 27) /
    //  (std * math.sqrt((numCells * 27 - (27 * 27)) / (numCells - 1.0)))
      
   //Michael
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
    
    //return 0.0 // g_score
  }*/
}
