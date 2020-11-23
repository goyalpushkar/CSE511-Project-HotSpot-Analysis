package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Entrance extends App {
  Logger.getLogger("org.spark_project").setLevel(Level.INFO)   //WARN Team14 changed
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  override def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("CSE512-HotspotAnalysis-Team14") // YOU NEED TO CHANGE YOUR GROUP NAME
      .config("spark.some.config.option", "some-value")
      .master("local[*]")   //Enable this to run locally and disable before generating jar file
      .getOrCreate()
    
    //paramsParser(spark, args)
    
    //Team14 Testing
    logger.info("Arguments - ")
    args.foreach(println)
    customTesting(spark, args)
    //Team14 Testing

  }
  
  //Custom function to test each method individually one by one
  private def customTesting(spark: SparkSession, args: Array[String]): Unit = {
     val outputPath = "test/output " //"result/output" //test/output 
     logger.info("Inside customTesting - ")
     //Run hotzoneanalysis - hotzoneanalysis src/resources/point_hotzone.csv src/resources/zone-hotzone.csv
     //Run hotcellanalysis - hotcellanalysis src/resources/yellow_trip_sample_100000.csv 
     var passedParams = ""
     if ( args(0).toString().toLowerCase().contains("hotzone") )
     {
        passedParams = args(1).toString() + " " + args(2).toString()
     }else{
       passedParams = args(1).toString()
     }
     
     
     val returnResult = queryLoader(spark
                                   ,args(0).toString()
                                   ,passedParams
                                   ,outputPath+"/"+args(0).toString()
                                   )
  }
  
  private def paramsParser(spark: SparkSession, args: Array[String]): Unit = {
    var paramOffset = 1
    var currentQueryParams = ""
    var currentQueryName = ""
    var currentQueryIdx = -1

    while (paramOffset <= args.length) {
      if (paramOffset == args.length || args(paramOffset).toLowerCase.contains("analysis")) {
        // Turn in the previous query
        if (currentQueryIdx != -1) queryLoader(spark, currentQueryName, currentQueryParams, args(0) + currentQueryIdx)

        // Start a new query call
        if (paramOffset == args.length) return

        currentQueryName = args(paramOffset)
        currentQueryParams = ""
        currentQueryIdx = currentQueryIdx + 1
      }
      else {
        // Keep appending query parameters
        currentQueryParams = currentQueryParams + args(paramOffset) + " "
      }
      paramOffset = paramOffset + 1
    }
  }

  private def queryLoader(spark: SparkSession, queryName: String, queryParams: String, outputPath: String) {
    val queryParam = queryParams.split(" ")
    if (queryName.equalsIgnoreCase("hotcellanalysis")) {
      if (queryParam.length != 1) throw new ArrayIndexOutOfBoundsException("[CSE512] Query " + queryName + " needs 1 parameters but you entered " + queryParam.length)
      HotcellAnalysis.runHotcellAnalysis(spark, queryParam(0)).limit(50).write.mode(SaveMode.Overwrite).csv(outputPath)  //.coalesce(1) Added for testing
    }
    else if (queryName.equalsIgnoreCase("hotzoneanalysis")) {
      if (queryParam.length != 2) throw new ArrayIndexOutOfBoundsException("[CSE512] Query " + queryName + " needs 2 parameters but you entered " + queryParam.length)
      HotzoneAnalysis.runHotZoneAnalysis(spark, queryParam(0), queryParam(1)).write.mode(SaveMode.Overwrite).csv(outputPath)  //.coalesce(1) Added for testing
    }
    else {
      throw new NoSuchElementException("[CSE512] The given query name " + queryName + " is wrong. Please check your input.")
    }
  }
}
