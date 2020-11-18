package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    //Get Rectangle's diagonally opposite corners and Point's X and Y coordinates by splitting passed string by "," and map it from String to Double value 
     val rectangelPoints = queryRectangle.split(",").map(f => f.toDouble)
     val point = pointString.split(",").map(f => f.toDouble)
     
     /*logger.info("rectangelPoints A - " + "("+ rectangelPoints(0) + "," + rectangelPoints(1) + ")" + "\n"
               + "rectangelPoints C - " + "("+ rectangelPoints(2)  + "," + rectangelPoints(3) + ")" + "\n"
               + "point - " + point
        )*/
     //Check if X and Y coordinates for the point lies between X and Y coordinates of 2 opposite end points of rectangle
     //Point's index 0 gives X coordinate for the point and rectangle's index 0 and 2  gives X coordinates of diagonally opposite ends
     //Point's index 1 gives Y coordinate for the point and rectangle's index 1 and 3  gives Y coordinates of diagonally opposite ends
     return rectangelPoints(0) <= point(0) && rectangelPoints(2) >= point(0) && rectangelPoints(1) <= point(1)  && rectangelPoints(3) >= point(1) 
     //return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
