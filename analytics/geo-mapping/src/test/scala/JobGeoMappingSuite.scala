import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class JobGeoMappingSuite extends FunSuite with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true
  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  import spark.implicits._

  //TODO: Move below test helper functions into a separate file?

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

    /*TODO: Do we need to check schema?*/
  }

  //---------- TESTS -----------
  test("Magellan Index input validation") {
    info("Default case")
    var inputString = "12,5,-1,19"
    var expectedResult = Seq(Some(10), Some(5), None, Some(15))
    var result = JobGeoMapping.validateMagellanIndex(inputString, ",")
    assert(result === expectedResult)

    // Different separator case
    inputString = "0#32#-10#6"
    expectedResult = Seq(None, Some(30), None, Some(5))
    result = JobGeoMapping.validateMagellanIndex(inputString, "#")
    assert(result === expectedResult)

    // Exception case
    inputString = "0,32,-10,6,A,"
    assertThrows[NumberFormatException] {
      result = JobGeoMapping.validateMagellanIndex(inputString, ",")
    }
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

  test("aggregateLabelsPerPoint test 1") {
    //aggregateLabelsPerPoint(df: DataFrame, groupByCols: Seq[String], aggegateCols: Seq[String]):  DataFrame

    val inputSeq = Seq(
      Row("User1", 53.350, -3.141, "120"),
      Row("User2", 53.373, -3.143, "213"),
      Row("User1", 53.350, -3.141, "125"),
      Row("User2", 53.373, -3.143, "220"),
      Row("User1", 53.350, -3.141, "130"),
      Row("User2", 53.373, -3.143, "230")
    )

    val schema = List(
      StructField("Name", StringType,false),
      StructField("Lat", DoubleType,false),
      StructField("Lng", DoubleType,false),
      StructField("Label1", StringType,false)
    )

    val dfIn1 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputSeq),
      StructType(schema)
    )

    var groupByCols = Seq("Name", "Lat", "Lng")
    var aggegateCols = Seq("Label1")
    val labelSeparator = ","

    var actualDf = JobGeoMapping.aggregateLabelsPerPoint(dfIn1, groupByCols, aggegateCols, labelSeparator)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,125,130"),
      Row("User2", 53.373, -3.143, "213,220,230")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("aggregateLabelsPerPoint test 2") {
    //aggregateLabelsPerPoint(df: DataFrame, groupByCols: Seq[String], aggegateCols: Seq[String]):  DataFrame

    val inputSeq = Seq(
      Row("User1", 53.350, -3.141, "120", "Name 1"),
      Row("User2", 53.373, -3.143, "213", "Name 2"),
      Row("User1", 53.350, -3.141, "125", "Name 3"),
      Row("User2", 53.373, -3.143, "220", "Name 2"),
      Row("User1", 53.350, -3.141, "130", "Name 1"),
      Row("User2", 53.373, -3.143, "230", "Name 3")
    )//.toDF("Name", "Lat", "Lng", "Label1", "Label2")

    val schema = List(
      StructField("Name", StringType,false),
      StructField("Lat", DoubleType,false),
      StructField("Lng", DoubleType,false),
      StructField("Label1", StringType,false),
      StructField("Label2", StringType,false)
    )

    val dfIn1 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputSeq),
      StructType(schema)
    )

    var groupByCols = Seq("Name", "Lat", "Lng")
    var aggegateCols = Seq("Label1", "Label2")
    val labelSeparator = ","

    var actualDf = JobGeoMapping.aggregateLabelsPerPoint(dfIn1, groupByCols, aggegateCols, labelSeparator)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,125,130", "Name 1,Name 3,Name 1"),
      Row("User2", 53.373, -3.143, "213,220,230", "Name 2,Name 2,Name 3")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }


  test("countLabelsPerPoint test") {
    val inputData = Seq(
      Row("User1", 53.350, -3.141, "120,130", "Name 1,Name 3,Name 1"),
      Row("User2", 53.373, -3.143, "213,220,230", "Name 2,Name 3")
    )

    val schema = List(
      StructField("Name", StringType,false),
      StructField("Lat", DoubleType,false),
      StructField("Lng", DoubleType,false),
      StructField("Label1", StringType,false),
      StructField("Label2", StringType,false)
    )

    val dfIn1 = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(schema)
    )

    /* Count label1 */
    var actualDf = JobGeoMapping.countLabelsPerPoint(dfIn1, "Label1", ",", "Label1Count")

    var expectedSchema = List(
      StructField("Name", StringType,false),
      StructField("Lat", DoubleType,false),
      StructField("Lng", DoubleType,false),
      StructField("Label1", StringType,false),
      StructField("Label2", StringType,false),
      StructField("Label1Count",IntegerType,false)
    )

    var expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,130", "Name 1,Name 3,Name 1", 2),
      Row("User2", 53.373, -3.143, "213,220,230", "Name 2,Name 3", 3)
    )

    var expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)

    /* Count label2 */
    actualDf = JobGeoMapping.countLabelsPerPoint(dfIn1, "Label2", ",", "Label2Count")

    expectedSchema = List(
      StructField("Name", StringType,false),
      StructField("Lat", DoubleType,false),
      StructField("Lng", DoubleType,false),
      StructField("Label1", StringType,false),
      StructField("Label2", StringType,false),
      StructField("Label2Count",IntegerType,false)
    )

    expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,130", "Name 1,Name 3,Name 1", 3),
      Row("User2", 53.373, -3.143, "213,220,230", "Name 2,Name 3", 2)
    )

    expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("runMultiPolygonsJob test 1 (Single polygon)") {

    /* This test case covers toGPS, Aggregate labels and job works with single polygons set.     * */

    val inputData = Seq(
      Row("User1", 324136.095, 384397.104),
      Row("User2", 324011.005, 386869.185),
      Row("User3", 325009.696, 386295.83)
    )

    val schema = List(
      StructField("Name", StringType,false),
      StructField("xCentroid", DoubleType,false),
      StructField("yCentroid", DoubleType,false)
    )

    val dfIn = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(schema)
    )

    val xColName           = "xCentroid"
    val yColName           = "yCentroid"
    val polygonsPath       = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val metadataToExtract  = Seq("UID")
    val magellanIndex      = Some(5)
    val aggregateLabels    = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = JobGeoMapping.loadMultiPolygons(spark, Seq(polygonsPath), Some(metadataToExtract), Seq(magellanIndex), None)
    var actualDf           = JobGeoMapping.runMultiPolygonJob(dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToExtract), Seq(magellanIndex), aggregateLabels, None, None)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedSchema = List(
      StructField("Name", StringType,false),
      StructField("xCentroid", DoubleType, false),
      StructField("yCentroid" ,DoubleType, false),
      StructField("lat",DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("UID", StringType, false),
      StructField("UID_Count", IntegerType, false)
    )

    val expectedData = Seq(
      Row("User1", 324136.095, 384397.104, 53.350996777067465,  -3.141149882762535, "213", 1),
      Row("User2", 324011.005, 386869.185, 53.373194945386025, -3.1436235641372563, "213", 1),
      Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "120,213", 2)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }


  test("runMultiPolygonsJob test 2 (multi polygon)") {

    val inputData = Seq(
      Row("User1", 324136.095, 384397.104),
      Row("User2", 324011.005, 386869.185),
      Row("User3", 325009.696, 386295.83)
    )

    val schema = List(
      StructField("Name", StringType,false),
      StructField("xCentroid", DoubleType,false),
      StructField("yCentroid", DoubleType,false)
    )

    val dfIn = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(schema)
    )

    val xColName           = "xCentroid"
    val yColName           = "yCentroid"
    val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
    val metadataToExtract  = Seq("UID", "Postdist,Postarea")
    val magellanIndex      = Seq(None, Some(5))
    val aggregateLabels    = true
    val polygonCol         = "polygon"
    val defaultPolygons    = None
    val toGPS              = true

    val dfMultiPolygons    = JobGeoMapping.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex, defaultPolygons)
    var actualDf           = JobGeoMapping.runMultiPolygonJob(dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateLabels, None, None)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedSchema = List(
      StructField("Name", StringType, true),
      StructField("xCentroid", DoubleType, true),
      StructField("yCentroid", DoubleType, true),
      StructField("lat", DoubleType, true),
      StructField("lon", DoubleType, true),
      StructField("UID", StringType, true),
      StructField("UID_Count", IntegerType, true),
      StructField("Postdist", StringType, true),
      StructField("Postarea", StringType, true),
      StructField("Postdist_Count", IntegerType, true)
    )

    val expectedData = Seq(
      Row("User1", 324136.095, 384397.104, 53.350996777067465, -3.141149882762535, "213", 1, "CH48", "CH", 1),
      Row("User2", 324011.005, 386869.185, 53.373194945386025, -3.1436235641372563, "213", 1, "CH48", "CH", 1),
      Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "120,213", 2, "CH48", "CH", 1)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("runMultiPolygonsJob test 3 (no GPS/aggregate)") {

    val inputData = Seq(
      Row("User1", -3.141149882762535, 53.350996777067465),
      Row("User2", -3.1436235641372563, 53.373194945386025),
      Row("User3", -3.128479626392792, 53.36818516353612)
    )

    val schema = List(
      StructField("Name", StringType,false),
      StructField("Lon", DoubleType,false),
      StructField("Lat", DoubleType,false)
    )

    val dfIn = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(schema)
    )

    val xColName           = "Lon"
    val yColName           = "Lat"
    val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
    val metadataToExtract  = Seq("UID", "Postdist,Postarea")
    val magellanIndex      = Seq(None, Some(5))
    val aggregateLabels    = false
    val polygonCol         = "polygon"
    val defaultPolygons    = None
    val toGPS              = false

    val dfMultiPolygons    = JobGeoMapping.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex, defaultPolygons)
    var actualDf           = JobGeoMapping.runMultiPolygonJob(dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateLabels, None, None)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedSchema = List(
      StructField("Name", StringType, true),
      StructField("Lon", DoubleType, true),
      StructField("Lat", DoubleType, true),
      StructField("UID", StringType, true),
      StructField("Postdist", StringType, true),
      StructField("Postarea", StringType, true)
      )

    val expectedData = Seq(
     Row("User1", -3.141149882762535, 53.350996777067465, "213", "CH48", "CH"),
     Row("User2", -3.1436235641372563, 53.373194945386025, "213", "CH48", "CH"),
     Row("User3", -3.128479626392792, 53.36818516353612, "120", "CH48", "CH"),
     Row("User3", -3.128479626392792, 53.36818516353612, "213", "CH48", "CH")
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("runMultiPolygonsJob test 4 (default polygons set)") {

    val inputData = Seq(
      Row("User1", -3.141149882762535, 53.350996777067465),
      Row("User2", -3.1436235641372563, 53.373194945386025),
      Row("User3", -3.128479626392792, 53.36818516353612)
    )

    val schema = List(
      StructField("Name", StringType,false),
      StructField("Lon", DoubleType,false),
      StructField("Lat", DoubleType,false)
    )

    val dfIn = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(schema)
    )

    val xColName           = "Lon"
    val yColName           = "Lat"
    val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
    val metadataToExtract  = Seq("UID", "Postdist,Postarea")
    val magellanIndex      = Seq(None, Some(5))
    val aggregateLabels    = true
    val polygonCol         = "polygon"
    val defaultPolygonsPath = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val toGPS              = false

    val dfMultiPolygons    = JobGeoMapping.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex, Some(defaultPolygonsPath))
    var actualDf           = JobGeoMapping.runMultiPolygonJob(dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateLabels, None, None)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedSchema = List(
      StructField("Name", StringType, true),
      StructField("Lon", DoubleType, true),
      StructField("Lat", DoubleType, true),
      StructField("UID", StringType, true),
      StructField("UID_Count", IntegerType, true),
      StructField("Postdist", StringType, true),
      StructField("Postarea", StringType, true),
      StructField("Postdist_Count", IntegerType, true)
    )

    val expectedData = Seq(
      Row("User1", -3.141149882762535, 53.350996777067465, "213,", 2, "CH48,", "CH,", 2),
      Row("User2", -3.1436235641372563, 53.373194945386025, "213,", 2, "CH48,", "CH,", 2),
      Row("User3", -3.128479626392792, 53.36818516353612, "120,213,", 3, "CH48,", "CH,", 2)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("removeSelectedLabel test") {
    val inputData = Seq(
      Row("User1", -3.141149882762535, 53.350996777067465, "213,", 2, "CH48,", "CH,", 2),
      Row("User2", -3.1436235641372563, 53.373194945386025, "213,", 2, "CH48,", "CH,", 2),
      Row("User3", -3.128479626392792, 53.36818516353612, "120,213,", 3, "CH48,", "CH,", 2)
    )

    val schema = List(
      StructField("Name", StringType, true),
      StructField("Lon", DoubleType, true),
      StructField("Lat", DoubleType, true),
      StructField("UID", StringType, true),
      StructField("UID_Count", IntegerType, true),
      StructField("Postdist", StringType, true),
      StructField("Postarea", StringType, true),
      StructField("Postdist_Count", IntegerType, true)
    )

    val dfIn = spark.createDataFrame(
      spark.sparkContext.parallelize(inputData),
      StructType(schema)
    )

    var actualDf = JobGeoMapping.removeSelectedLabel(dfIn, "UID", ",", "")
    actualDf = JobGeoMapping.removeSelectedLabel(actualDf, "Postdist", ",", "")
    actualDf = JobGeoMapping.removeSelectedLabel(actualDf, "Postarea", ",", "")

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", -3.141149882762535, 53.350996777067465, "213", 2, "CH48", "CH", 2),
      Row("User2", -3.1436235641372563, 53.373194945386025, "213", 2, "CH48", "CH", 2),
      Row("User3", -3.128479626392792, 53.36818516353612, "120,213", 3, "CH48", "CH", 2)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }
}
