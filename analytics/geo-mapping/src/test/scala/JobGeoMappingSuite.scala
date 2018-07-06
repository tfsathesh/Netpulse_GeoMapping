import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

class JobGeoMappingSuite extends FunSuite with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  import spark.implicits._

  test("Magellan Index input validation") {
    info("Default case")
    var inputString = "12,5,-1,19"
    var expectedResult = Seq(Some(10), Some(5), None, Some(15))
    var result = JobGeoMapping.validateMagellanIndex(inputString, ",")
    assert(result === expectedResult)

    info("Different separator case")
    inputString = "0#32#-10#6"
    expectedResult = Seq(None, Some(30), None, Some(5))
    result = JobGeoMapping.validateMagellanIndex(inputString, "#")
    assert(result === expectedResult)

    info("Exception case")
    inputString = "0,32,-10,6,A,"
    assertThrows[NumberFormatException] {
      result = JobGeoMapping.validateMagellanIndex(inputString, ",")
    }
  }

  /**
    *
    *
    * @param
    * @return
    */
  def loadMultiPolygonsTest(spark:SparkSession,
                            multiPolygonsPath:Seq[String],
                            multiPolygonsMetadataToExtract:Option[Seq[String]] = None,
                            magellanIndex:Seq[Option[Int]] = Seq(None),
                            defaultPolygonsPath: Option[String] = None
                           ): Unit = {

    val expectedPolygonSetCount = multiPolygonsPath.length
    // Load with out default set
    val loadedPolygonsDf = JobGeoMapping.loadMultiPolygons(spark, multiPolygonsPath, multiPolygonsMetadataToExtract, magellanIndex, None)

    assert(loadedPolygonsDf.length === expectedPolygonSetCount)

    // Verify polygons loaded and dataframe contains metadata columns.
    for ((polygonsDf, metadataToExtract) <- loadedPolygonsDf.zip(multiPolygonsMetadataToExtract.get)) {
      assertTrue(polygonsDf.count > 1)
      assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToExtract.split(",")))
    }

    // Get polygons count without default set
    val polygonsCount = for (polygonsDf <- loadedPolygonsDf) yield {
      polygonsDf.count
    }

    // Load polygons with default path
    val loadedPolygonsWithDefaultSetDf = JobGeoMapping.loadMultiPolygons(spark, multiPolygonsPath, multiPolygonsMetadataToExtract, magellanIndex, defaultPolygonsPath)

    // Verify polygons loaded and dataframe contains metadata columns.
    for ((polygonsDf, metadataToExtract) <- loadedPolygonsWithDefaultSetDf.zip(multiPolygonsMetadataToExtract.get)) {
      assertTrue(polygonsDf.count > 1)
      assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToExtract.split(",")))
    }

    // Get polygons count with default set
    val polygonsCountWithDefaultSet = for (polygonsDf <- loadedPolygonsWithDefaultSetDf) yield {
      polygonsDf.count
    }

    // Check polygons count has been increased by 1
    for( (countWithout, countWith) <- polygonsCount.zip(polygonsCountWithDefaultSet)) {
      assert( (countWithout + 1) === countWith)
    }

    /*TODO: Do we need to check schema and its data type?*/
  }

  test("loadMultiPolygons test 1") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val multiPolygonsMetadataToExtract  = Seq("RadioManager,UID,Optimiser,CHUNK,AREA_OP,AREA_OWNER",
                                              "Postdist,Postarea",
                                              "name")
    val magellanIndex                   = Seq(Some(5), None, Some(15))
    val defaultPolygonsPath             = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(multiPolygonsMetadataToExtract), magellanIndex, Some(defaultPolygonsPath))
  }

  test("loadMultiPolygons test 2 (single path)") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1)
    val multiPolygonsMetadataToExtract  = Seq("RadioManager,UID,Optimiser,CHUNK,AREA_OP,AREA_OWNER")
    val magellanIndex                   = Seq(Some(5))
    val defaultPolygonsPath             = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(multiPolygonsMetadataToExtract), magellanIndex, Some(defaultPolygonsPath))
  }

  ignore("loadMultiPolygons test with shp files") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("shapefiles/beacon_gps_sample/").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("shapefiles/postdist_gps_sample/").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("shapefiles/districts_gps_sample/").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val multiPolygonsMetadataToExtract  = Seq("RadioManager,UID,Optimiser,CHUNK,AREA_OP,AREA_OWNER",
                                              "Postdist,Postarea",
                                              "name")
    val magellanIndex                   = Seq(Some(10), Some(5), None)
    val defaultPolygonsPath             = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(multiPolygonsMetadataToExtract), magellanIndex, Some(defaultPolygonsPath))
  }


  test("combineLabeledDataframes test 1") {
    val df1 = Seq(
      ("User1", 53.350, -3.141, 120),
      ("User2", 53.373, -3.143, 213),
      ("User3", 53.359, -3.168, 120)
    ).toDF("Name", "Lat", "Lng", "Label1")

    val df2 = Seq(
      ("User1", 53.350, -3.141, "SL1"),
      ("User2", 53.373, -3.143, "DB2"),
      ("User3", 53.359, -3.168, "PG3")
    ).toDF("Name", "Lat", "Lng", "Label2")

    val joinCols = Seq("Name", "Lat", "Lng")

    var actualDf = JobGeoMapping.combineLabeledDataframes(Seq(df1, df2), joinCols)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedSchema = List(
      StructField("Name", StringType,true),
      StructField("Lat", DoubleType,true),
      StructField("Lng", DoubleType,true),
      StructField("Label1", IntegerType,true),
      StructField("Label2", StringType,true))

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, 120, "SL1"),
      Row("User2", 53.373, -3.143, 213, "DB2"),
      Row("User3", 53.359, -3.168, 120, "PG3")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.05)

  }

  test("combineLabeledDataframes test 2") {
    val df1 = Seq(
      ("User1", 53.350, -3.141, 120),
      ("User2", 53.373, -3.143, 213)
    ).toDF("Name", "Lat", "Lng", "Label1")

    val df2 = Seq(
      ("User1", 53.350, -3.141, 213),
      ("User2", 53.373, -3.143, 120),
      ("User4", 53.386,	-3.160, 213)
    ).toDF("Name", "Lat", "Lng", "Label2")

    val joinCols = Seq("Name", "Lat", "Lng")

    var actualDf = JobGeoMapping.combineLabeledDataframes(Seq(df1, df2), joinCols)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedSchema = List(
      StructField("Name", StringType,true),
      StructField("Lat", DoubleType,true),
      StructField("Lng", DoubleType,true),
      StructField("Label1", IntegerType,true),
      StructField("Label2", IntegerType,true))

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, 120, 213),
      Row("User2", 53.373, -3.143, 213, 120),
      Row("User4", 53.359, -3.168, null, 213)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.05)
  }

  test("combineLabeledDataframes test 3 (Incorrect column name)") {
    val df1 = Seq(
      ("User1", 53.350, -3.141, 120),
      ("User2", 53.373, -3.143, 213)
    ).toDF("Name", "Lat", "Lng", "Label1")

    val df2 = Seq(
      ("User1", 53.350, -3.141, 213),
      ("User2", 53.373, -3.143, 120),
      ("User4", 53.386, -3.160, 213)
    ).toDF("Name", "Lat", "Lng", "Label2")

    val joinCols = Seq("Name", "Lat", "IncorrectColName")

    assertThrows[org.apache.spark.sql.AnalysisException] {
      JobGeoMapping.combineLabeledDataframes(Seq(df1, df2), joinCols)
    }
  }

  ignore("aggregateLabelsPerPoint test 1") {
    //aggregateLabelsPerPoint(df: DataFrame, groupByCols: Seq[String], aggegateCols: Seq[String]):  DataFrame

    val dfIn11 = Seq(
      ("User1", 53.350, -3.141, "120"),
      ("User2", 53.373, -3.143, "213"),
      ("User1", 53.350, -3.141, "125"),
      ("User2", 53.373, -3.143, "220"),
      ("User1", 53.350, -3.141, "130"),
      ("User2", 53.373, -3.143, "230")
    ).toDF("Name", "Lat", "Lng", "Label1")

    var groupByCols = Seq("Name", "Lat", "Lng")
    var aggegateCols = Seq("Label1")

    var actualDf = JobGeoMapping.aggregateLabelsPerPoint(dfIn11, groupByCols, aggegateCols)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,125,130"),
      Row("User2", 53.373, -3.143, "213,220,230")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      dfIn11.schema
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  ignore("aggregateLabelsPerPoint test 2") {
    //aggregateLabelsPerPoint(df: DataFrame, groupByCols: Seq[String], aggegateCols: Seq[String]):  DataFrame

    val dfIn11 = Seq(
      ("User1", 53.350, -3.141, "120", "Name 1"),
      ("User2", 53.373, -3.143, "213", "Name 2"),
      ("User1", 53.350, -3.141, "125", "Name 3"),
      ("User2", 53.373, -3.143, "220", "Name 2"),
      ("User1", 53.350, -3.141, "130", "Name 1"),
      ("User2", 53.373, -3.143, "230", "Name 3")
    ).toDF("Name", "Lat", "Lng", "Label1", "Label2")

    var groupByCols = Seq("Name", "Lat", "Lng")
    var aggegateCols = Seq("Label1")

    var actualDf = JobGeoMapping.aggregateLabelsPerPoint(dfIn11, groupByCols, aggegateCols)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,125,130"),
      Row("User2", 53.373, -3.143, "213,220,230")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      dfIn11.schema
    ).sort("Name")

    log.warn(actualDf.show)
    log.warn(expectedDF.show)
    assert(1 === 1)

  }
}
