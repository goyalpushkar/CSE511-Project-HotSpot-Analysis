package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)  //WARN Modified by Team14 Reverted
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  logger.info("Inside runHotcellAnalysis")
  
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  
  logger.info("pickup info raw") //Team14 Testing
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
  
  logger.info("pickup info after derivation") //Team14 Testing
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  
   logger.info("numCells - " + numCells) //Team14 Testing
  // YOU NEED TO CHANGE THIS PART
  
   // Pushkar
  //Get all the cells i.e Pickup Points within the given boundary and take their counts
  pickupInfo.createOrReplaceTempView("pick_points")
  val cellCountQuery = s""" SELECT pp.x, pp.y, pp.z, COUNT(1) count
                               FROM pick_points pp
                              WHERE pp.x BETWEEN $minX AND $maxX
                                AND pp.y BETWEEN $minY AND $maxY
                                AND pp.z BETWEEN $minZ AND $maxZ
                              GROUP BY pp.x, pp.y, pp.z
                         """
  logger.info("cellCountQuery - " + cellCountQuery)
  val cellCounts = spark.sql(cellCountQuery)  
  cellCounts.createOrReplaceTempView("eligible_cells")
  //logger.info("Eligible Cell Count - " + cellCounts.count().toString() )
  cellCounts.show()
  
  //Get Mean and Standard deviation of all the cells i.e Pickup Points
  /*val cellMeanStdQuery = s""" SELECT ( SUM(count) / $numCells ) mean
                                    ,sqrt( ( SUM( power(count, 2) ) / $numCells ) - power( ( SUM(count) / $numCells ) , 2) ) std_dev
                                FROM eligible_cells
                           """
  logger.info("cellMeanStdQuery - " + cellMeanStdQuery)
  val cellMeanStd = spark.sql(cellMeanStdQuery)
  //logger.info(cellMeanStd.count())
  cellMeanStd.show()
  //val cellMean1: BigDecimal = scala.math.BigDecimal.valueOf(cellMeanStd.first().apply(0).asInstanceOf[java.lang.Double] )   //math.BigDecimal
  //val cellDeviation1: BigDecimal = scala.math.BigDecimal.valueOf(cellMeanStd.first().apply(1).asInstanceOf[java.lang.Double])
  */
  val cellMean: Double = cellCounts.agg(sum("count") / numCells).first.getDouble(0)
  val cellDeviation: Double = math.sqrt(cellCounts.agg(sum(pow("count", 2.0)) / numCells - math.pow(cellMean, 2.0)).first.getDouble(0))

  logger.info("mean - " + cellMean + " :std - " + cellDeviation)
    
  //Get all the neighbours
  spark.udf.register("is_neighbour", (x1: Double, y1: Double, z1: Double, x2: Double, y2: Double, z2: Double) => 
       HotcellUtils.is_neighbor(x1, y1, z1, x2, y2, z2) )
  val neighboursQuery = s""" SELECT ec1.x, ec1.y, ec1.z  --, ec1.count self_count, ec2.count neighbour_count
                                   ,SUM(ec2.count) spatial_weight_of_neighbours
                               FROM eligible_cells ec1
                                   ,eligible_cells ec2
                              WHERE is_neighbour(ec1.x, ec1.y, ec1.z, ec2.x, ec2.y, ec2.z)
                            GROUP BY ec1.x, ec1.y, ec1.z
                         """
  logger.info("neighboursQuery - " + neighboursQuery)
  val neighbours = spark.sql(neighboursQuery)
  neighbours.createOrReplaceTempView("eligible_neighbours")
  //logger.info("Neighbor Count - " + neighbours.count())
  neighbours.show()
  
  //Get Getis Ord by using the regsitered getis_ord udf (User defined function)
  spark.udf.register("get_num_of_neighbours", (x: Double, y: Double, z: Double) => 
    HotcellUtils.get_num_of_neighbours(x, y, z, minX, maxX, minY, maxY, minZ, maxZ) )
  spark.udf.register("getis_ord_cal", (spatial_neighbor_weight: Double, noOfNeighbors: Int) => 
    HotcellUtils.getis_ord_cal(spatial_neighbor_weight, noOfNeighbors, cellMean, cellDeviation, numCells) )
  val getisOrdQuery = s"""WITH base_rows AS
                         (SELECT x, y, z
                                ,getis_ord_cal(spatial_weight_of_neighbours, get_num_of_neighbours(x, y, z) ) z_score
                            FROM eligible_neighbours en
                         )
                        SELECT x, y, z
                          FROM base_rows
                      ORDER BY z_score DESC, x DESC, y ASC, z DESC
                      """
  
  /*spark.udf.register("getis_ord", (spatial_neighbor_weight: Double) => 
    HotcellUtils.getis_ord(spatial_neighbor_weight, cellMean, cellDeviation) )  //It is not passing all test cases only 13/15 are passing
  val getisOrdQuery = s"""WITH base_rows AS
                          ( SELECT x, y, z, getis_ord(spatial_weight_of_neighbours) z_score
                              FROM eligible_neighbours
                          )
                        SELECT x, y, z
                          FROM base_rows
                      ORDER BY z_score DESC, x DESC, y ASC, z DESC
                      """
                       */
  logger.info("getisOrdQuery - " + getisOrdQuery)
  val getisOrd = spark.sql(getisOrdQuery)
  getisOrd.createOrReplaceTempView("getisOrd")
  //logger.info("Getis Ord Count - " +getisOrd.count())
  getisOrd.show()
  
  //Drop Z score and sort the result by required format
  //val getisOrdFinal = getisOrd.drop("z_score")
  
  /* Candice
  // obtain all the pickup points x, y, z within the given boundary
  val filtered_points = pickupInfo
    .filter(col("x")>= minX and col("x")<= maxX and col("y") >= minY and col("y") <= maxY and col("z") >= minZ and col("z") <= maxZ)
    .groupBy("x", "y", "z")
    .count()
    .persist()
  filtered_points.createOrReplaceTempView("hotcell_view")
  //calculate mean and standard deviation
  val mean: Double = filtered_points.agg(sum("count") / numCells).first.getDouble(0)
  val std: Double = math.sqrt(filtered_points.agg(sum(pow("count", 2.0)) / numCells - math.pow(mean, 2.0)).first.getDouble(0))

  //Cross join
  spark.udf.register("is_neighbor", (x1: Int, y1: Int, z1: Int, x2: Int, y2: Int, z2: Int) => (HotcellUtils.is_neighbor(x1, y1, z1, x2, y2, z2)))
  var cross_join = spark.sql("SELECT i.x AS x, i.y AS y, i.z AS z, j.count AS count FROM hotcell_view i, hotcell_view j WHERE is_neighbor(i.x, i.y, i.z, j.x, j.y, j.z)")
  cross_join = cross_join.groupBy("x", "y", "z").sum("count")
  newCoordinateName = Seq("x", "y", "z", "sum_neigh")
  cross_join = cross_join.toDF(newCoordinateName: _*)
  cross_join.createOrReplaceTempView("cross_join")

  //Get g-score
  spark.udf.register("getis_ord", (sum_neigh: Int) => HotcellUtils.getis_ord(numCells, mean, std, sum_neigh))
  var gScore = spark.sql("SELECT x, y, z, getis_ord(sum_neigh) AS gscore FROM cross_join")
  gScore.createOrReplaceTempView("gscore_view")
  var final_result = spark.sql("SELECT x, y, z FROM gscore_view  ORDER BY gscore DESC, x DESC, y ASC, z DESC")
  */
  
  //Michael
  /*pickupInfo.createOrReplaceTempView("pinfo")
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
 logger.info("mean - " + mean + " :std - " + std)
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
  gScore.show()
  var getisOrd = spark.sql("SELECT x, y, z, gscore FROM gscore_view ORDER BY gscore DESC, x DESC, y ASC, z DESC")
  getisOrd.show()*/

  return getisOrd.coalesce(1)   //getisOrd.coalesce(1)   //pickupInfo // YOU NEED TO CHANGE THIS PART
}
}
