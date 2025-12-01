package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty()) {
      return false
    }
    
    val rectCoords = queryRectangle.split(",")
    if (rectCoords.length != 4) return false
    
    val rectX1 = rectCoords(0).trim.toDouble
    val rectY1 = rectCoords(1).trim.toDouble
    val rectX2 = rectCoords(2).trim.toDouble
    val rectY2 = rectCoords(3).trim.toDouble
    
    val pointCoords = pointString.split(",")
    if (pointCoords.length != 2) return false
    
    val pointX = pointCoords(0).replace("(", "").trim.toDouble
    val pointY = pointCoords(1).replace(")", "").trim.toDouble
    
    val minX = Math.min(rectX1, rectX2)
    val maxX = Math.max(rectX1, rectX2)
    val minY = Math.min(rectY1, rectY2)
    val maxY = Math.max(rectY1, rectY2)
    
    return pointX >= minX && pointX <= maxX && pointY >= minY && pointY <= maxY
    // YOU NEED TO CHANGE THIS PART
  }
}