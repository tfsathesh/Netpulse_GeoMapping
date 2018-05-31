package com.o2.uds

import magellan.{Point, PolyLine, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.io._
import org.apache.spark.sql.functions.trim

import org.apache.log4j._
import org.apache.spark.SparkConf

object MagellanDemo {
  def main(args: Array[String]) {

    // Validate command line arguments
    if(args.length != 1) {
      println(" Incorrect Arguments or Config Properties file path missed!!!")
      System.exit(0)
    }

    // Master, AppName and other papmeters to be set from spark-submit
    val spark = SparkSession.builder()
    .getOrCreate()

    // For implicit conversions
    import spark.implicits._

    // Get logger
    //TBD

    // Read config properties
    val configProperties = new Properties()
    configProperties.load(new FileInputStream(args(0)))   // config.properties file from command line arguments.
    val inputPointsDataPath = configProperties.getProperty("InputPointsDataPath")
    val headerPresent = configProperties.getProperty("HeaderPresent").toBoolean
    val delimiterString = configProperties.getProperty("DelimiterString")
    val inputGeoDataPath = configProperties.getProperty("InputGeoDataPath")
    val outputResultsDataPath = configProperties.getProperty("OutputResultsDataPath")
    val indexPrecision = configProperties.getProperty("IndexPrecision").toInt


    // Remove the header if exist in the input file
    val pointsRDD = spark.read.textFile(inputPointsDataPath)

    val pointsTransformedRDD = if(headerPresent) {
      val header = pointsRDD.first()
      pointsRDD.filter(line => line != header)
    } else {
      pointsRDD
    }

    // Points data
    val pointsDF = pointsTransformedRDD.map { line =>
      val parts = line.split(delimiterString)
      // These column numbers based on test data set.
      // Need to revisit when we get actual points data.
      val point = Point(parts(3).toDouble, parts(4).toDouble)
      PointRecord(point)
  }.toDF.cache

    // UK Sectors data
    val ukSectorsDF  = spark.read.format("magellan")
        .load(inputGeoDataPath).select($"polygon", $"metadata").cache()

    val joinResDF =  if(indexPrecision > 0) {

      val ukSectorsIndexedDF = ukSectorsDF.index(indexPrecision)
      // Set the rule
      magellan.Utils.injectRules(spark)
      // Join with geo indexing
     pointsDF.join(ukSectorsIndexedDF index indexPrecision).where($"point" within $"polygon")
        .select( $"point", explode($"metadata").as(Seq("key", "value"))).withColumnRenamed("value", "Name")
        .withColumn("Name", trim($"Name")).drop("key")

    } else {

      // Join without geo indexing
      pointsDF.join(ukSectorsDF).where($"point" within $"polygon")
        .select( $"point", explode($"metadata").as(Seq("key", "value"))).withColumnRenamed("value", "Name")
        .withColumn("Name", trim($"Name")).drop("key")
    }

    // Save data json
    joinResDF.write.format("json").mode(SaveMode.Overwrite).save(outputResultsDataPath)

    // Stop spark session.
    spark.stop()

  }
  // class for Point
  case class PointRecord(point: magellan.Point)
 }