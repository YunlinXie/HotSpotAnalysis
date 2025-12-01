package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    ////////////////////////////////////////////////////////////////////////
    // YOU NEED TO CHANGE THIS PART
    pickupInfo.createOrReplaceTempView("pickupInfo")

    // Register UDF for neighbor check using HotcellUtils
    spark.udf.register("isNeighbor", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => {
      HotcellUtils.areCellsAdjacent(x1, y1, z1, x2, y2, z2)
    })

    // Get cell counts within the defined bounds
    val cellCounts = spark.sql(s"""
      select x, y, z, count(*) as countValue 
      from pickupInfo 
      where x >= $minX and x <= $maxX and y >= $minY and y <= $maxY and z >= $minZ and z <= $maxZ
      group by x, y, z
    """)
    cellCounts.createOrReplaceTempView("cellCounts")

    // Calculate total points and mean - FIXED: Proper type handling
    val totalPointsRow = cellCounts.agg(sum("countValue")).collect()(0)
    val totalPoints = totalPointsRow.get(0) match {
      case l: Long => l.toDouble
      case d: Double => d
      case _ => 0.0
    }
    val mean = totalPoints / numCells

    // Calculate standard deviation - FIXED: Proper type handling
    val sumSquaresRow = cellCounts.agg(sum(col("countValue") * col("countValue"))).collect()(0)
    val sumSquares = sumSquaresRow.get(0) match {
      case l: Long => l.toDouble
      case d: Double => d
      case _ => 0.0
    }
    val stdDev = Math.sqrt((sumSquares / numCells) - (mean * mean))

    // Create all possible cells within the spatial-temporal bounds
    val allXCells = spark.range(minX.toInt, maxX.toInt + 1).select(col("id").as("x"))
    val allYCells = spark.range(minY.toInt, maxY.toInt + 1).select(col("id").as("y"))  
    val allZCells = spark.range(minZ, maxZ + 1).select(col("id").as("z"))
    
    val allCells = allXCells.crossJoin(allYCells).crossJoin(allZCells)
    allCells.createOrReplaceTempView("allCells")

    // Join with actual counts (cells with no points get count 0)
    val allCellCounts = allCells.join(cellCounts, Seq("x", "y", "z"), "left")
      .na.fill(0, Seq("countValue"))
    allCellCounts.createOrReplaceTempView("allCellCounts")

    // Calculate sum of counts for all neighbors of each cell
    val neighborSums = spark.sql("""
      select c1.x as x, c1.y as y, c1.z as z, 
             sum(c2.countValue) as neighborSum,
             count(c2.x) as neighborCount
      from allCellCounts c1
      cross join allCellCounts c2
      where isNeighbor(c1.x, c1.y, c1.z, c2.x, c2.y, c2.z)
      group by c1.x, c1.y, c1.z
    """)
    neighborSums.createOrReplaceTempView("neighborSums")

    // Calculate G-scores using SQL to avoid type casting issues
    val gScores = spark.sql(s"""
      select x, y, z,
             (neighborSum - $mean * neighborCount) / 
             ($stdDev * sqrt(($numCells * neighborCount - neighborCount * neighborCount) / ($numCells - 1.0))) as gScore
      from neighborSums
      where neighborCount > 0
    """)
    gScores.createOrReplaceTempView("gScores")

    // Get top 50 hottest cells sorted by G-score in descending order
    val result = spark.sql("select x, y, z from gScores order by gScore desc limit 50")

    return result 
    // YOU NEED TO CHANGE THIS PART
  }
}