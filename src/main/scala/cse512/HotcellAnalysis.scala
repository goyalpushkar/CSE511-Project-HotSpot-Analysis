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

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
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

  pickupInfo.createOrReplaceTempView("pinfo")

  // YOU NEED TO CHANGE THIS PART

//  val filtered_points = pickupInfo
//    .filter(col("x")>= minX and col("x")<= maxX and col("y") >= minY and col("y") <= maxY and col("z") >= minZ and col("z") <= maxZ)
//    .groupBy("x", "y", "z")
//    .count()
//    .persist()
  val filtered_points = spark.sql("SELECT x,y,z from pinfo where x>= "+minX+" and x<= "+maxX+" and y>= "+minY+" and y<="+maxY+" and z>= "+minZ+" and z<= "+maxZ+"")
  filtered_points.createOrReplaceTempView("hotcell_v")
  val filtered_points2 = spark.sql("SELECT x,y,z,count(*) as count from hotcell_v GROUP BY x,y,z")
  filtered_points2.createOrReplaceTempView("hotcell_view")
  //calculate mean and standard deviation
  val mean: Double = filtered_points2.agg(sum("count") / numCells).first.getDouble(0)
  val std: Double = math.sqrt(filtered_points2.agg(sum(pow("count", 2.0)) / numCells - math.pow(mean, 2.0)).first.getDouble(0))
  //Cross join

  spark.udf.register("is_neighbor", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => (HotcellUtils.is_neighbor(x1, y1, z1, x2, y2, z2)))

  var cross_join = spark.sql("SELECT i.x AS x, i.y AS y, i.z AS z, j.count AS count FROM hotcell_view i, hotcell_view j WHERE is_neighbor(i.x, i.y, i.z, j.x, j.y, j.z)")
  cross_join = cross_join.groupBy("x", "y", "z").sum("count")
  newCoordinateName = Seq("x", "y", "z", "sum_neigh")
  cross_join = cross_join.toDF(newCoordinateName: _*)
  cross_join.createOrReplaceTempView("cross_join")

  //Get g-score
  spark.udf.register("getis_ord", (sum_neigh: Int,x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) =>
    HotcellUtils.getis_ord(numCells, mean, std, sum_neigh, x,y,z,minX,maxX,minY,maxY,minZ,maxZ))
  var gScore = spark.sql("SELECT x, y, z, getis_ord(sum_neigh, x,y,z," + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ")" +
    " AS gscore FROM cross_join")
  gScore.createOrReplaceTempView("gscore_view")
  var final_result = spark.sql("SELECT x, y, z FROM gscore_view ORDER BY gscore DESC, x DESC, y ASC, z DESC")
  gScore.show()

  return final_result//.coalesce(1)
}
}
