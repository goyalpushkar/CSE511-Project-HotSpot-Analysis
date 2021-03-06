package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)  //INFO Modified by Team14 Reverted
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {
    
    logger.info("Inside runHotZoneAnalysis")
    
    var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf.createOrReplaceTempView("point")
    
    logger.info("Point Data Set")
    pointDf.show(10) //Team14
    
    // Parse point data formats
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")
    
    logger.info("Point Data Set after trimming")
    pointDf.show(10) //Team14 

    // Load rectangle data
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")
    
    logger.info("Rectangle Data Set")
    rectangleDf.show(10)  //Team14
    
    // Join two datasets
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
    joinDf.createOrReplaceTempView("joinResult")

    logger.info("Joined Data Set")
    joinDf.show(10)
    
    // YOU NEED TO CHANGE THIS PART
    //Count number of points in each rectangle by grouping the join result on rectangle
    val hotzone = spark.sql("""SELECT rectangle, COUNT(point) no_of_points FROM joinResult GROUP BY rectangle ORDER BY rectangle ASC""")
    logger.info("HotZone Data Set")
    hotzone.show(10)
    
    return hotzone.coalesce(1)  //joinDf // YOU NEED TO CHANGE THIS PART
  }

}
