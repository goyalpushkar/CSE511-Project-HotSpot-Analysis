package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle:String, pointString: String):Boolean = {
    val Array(x1, y1, x2, y2) = queryRectangle.split(",").map(s => s.toDouble)
    val Array(pointx, pointy) = pointString.split(",").map(s => s.toDouble)
    if (pointx >= x1 && pointx <= x2 && y1 <= pointy && pointy <= y2) return true
    else return false
  }

  // YOU NEED TO CHANGE THIS PART

}
