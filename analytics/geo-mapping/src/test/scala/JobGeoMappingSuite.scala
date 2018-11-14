import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class JobGeoMappingSuite extends FunSuite with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true

  import spark.implicits._

  test("Magellan Index input check") {

    var inputString = "12,5,-1,19"
    var expectedResult = Seq(Some(10), Some(5), None, Some(15))
    var result = JobGeoMapping.convertStringIndexToIntSeq(inputString, ",")
    assert(result === expectedResult)

    // Different separator case
    inputString = "0#32#-10#6"
    expectedResult = Seq(None, Some(30), None, Some(5))
    result = JobGeoMapping.convertStringIndexToIntSeq(inputString, "#")
    assert(result === expectedResult)

    inputString = "12,,,19"
    expectedResult = Seq(Some(10), None, None, Some(15))
    result = JobGeoMapping.convertStringIndexToIntSeq(inputString, ",")
    assert(result === expectedResult)

    // Exception case
    inputString = "0,32,-10,6,A,"
    assertThrows[NumberFormatException] {
      result = JobGeoMapping.convertStringIndexToIntSeq(inputString, ",")
    }
  }

  test("removeDefaultMetadata test") {
    //(metadataInString: String, metadataToRemove: String, metadataSeparator: String) : String = {
    var metadataInString    = "123,,180,,46,,47,"
    var metadataToRemove    = ""
    var metadataSeparator   = ","
    var expectedRes         = "123,180,46,47"

    var actualRes = JobGeoMapping.removeDefaultMetadata(metadataInString, metadataToRemove, metadataSeparator)
    assert(expectedRes, actualRes)

    metadataInString    = "123,111,180,111,46,111,47,"
    metadataToRemove    = "111"
    metadataSeparator   = ","
    expectedRes         = "123,180,46,47"

    actualRes = JobGeoMapping.removeDefaultMetadata(metadataInString, metadataToRemove, metadataSeparator)
    assert(expectedRes, actualRes)

    metadataInString    = "123#111#180#111#46#111#47,"
    metadataToRemove    = "111"
    metadataSeparator   = "#"
    expectedRes         = "123#180#46#47,"

    actualRes = JobGeoMapping.removeDefaultMetadata(metadataInString, metadataToRemove, metadataSeparator)
    assert(expectedRes, actualRes)
  }

  test ("convertStringIndexToIntSeq test") {
    //index: String, separator: String) : Seq[Option[Int]]

    var indexString                      = "5,5,5"
    var separator                        = ","
    var expectedRes:Seq[Option[Int]]     = Seq(Some(5), Some(5), Some(5))

    var actualRes       = JobGeoMapping.convertStringIndexToIntSeq(indexString, separator)
    assert(expectedRes, actualRes)

    indexString     = "5,4,6"
    separator       = ","
    expectedRes     = Seq(Some(5), None, Some(5))
    actualRes       = JobGeoMapping.convertStringIndexToIntSeq(indexString, separator)
    assert(expectedRes, actualRes)

    indexString     = "0,-1,16"
    separator       = ","
    expectedRes     = Seq(None, None, Some(15))
    actualRes       = JobGeoMapping.convertStringIndexToIntSeq(indexString, separator)
    assert(expectedRes, actualRes)

    indexString     = "100, ,-50, "
    separator       = ","
    expectedRes     = Seq(Some(100), None, None, None)
    actualRes       = JobGeoMapping.convertStringIndexToIntSeq(indexString, separator)
    assert(expectedRes, actualRes)
  }

  test("consolidateMetadata single metadata field") {
    val dfIn = Seq(
      ("User1", 53.350, -3.141, "120"),
      ("User2", 53.373, -3.143, "213"),
      ("User1", 53.350, -3.141, "125"),
      ("User2", 53.373, -3.143, "220"),
      ("User1", 53.350, -3.141, "130"),
      ("User2", 53.373, -3.143, "230")
    ).toDF("Name", "Lat", "Lng", "Label1")

    val groupByCols = Seq("Name", "Lat", "Lng")
    val aggregateCols = Seq("Label1")

    var actualDf = JobGeoMapping.consolidateMetadata(dfIn, groupByCols, aggregateCols, JobGeoMapping.METADATA_SEPARATOR, None)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,125,130", 3),
      Row("User2", 53.373, -3.143, "213,220,230", 3)
    )

    //TODO: Do we need to have expected schema well?

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

    test("consolidateMetadata multiple metadata fields") {
      val dfIn = Seq(
        ("User1", 53.350, -3.141, "120", "Name 1"),
        ("User2", 53.373, -3.143, "213", "Name 2"),
        ("User1", 53.350, -3.141, "125", "Name 3"),
        ("User2", 53.373, -3.143, "220", "Name 2"),
        ("User1", 53.350, -3.141, "130", "Name 1")
      ).toDF("Name", "Lat", "Lng", "Label1", "Label2")

      val groupByCols = Seq("Name", "Lat", "Lng")
      val aggregateCols = Seq("Label1", "Label2")

      var actualDf = JobGeoMapping.consolidateMetadata(dfIn, groupByCols, aggregateCols, JobGeoMapping.METADATA_SEPARATOR, None)

      // Sort to keep the order (needed for dataframe comparision)
      actualDf = actualDf.sort("Name")

      //TODO: Do we need to have expected schema as well?
      val expectedData = Seq(
        Row("User1", 53.350, -3.141, "120,125,130", "Name 1,Name 3,Name 1", 3),
        Row("User2", 53.373, -3.143, "213,220", "Name 2,Name 2", 2)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(actualDf.schema)
      ).sort("Name")

      assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
    }


  test("consolidateMetadata with default metadata") {
    val dfIn = Seq(
      ("User1", 53.350, -3.141, "120,", "Name 1,"),
      ("User2", 53.373, -3.143, "213", "Name 2"),
      ("User1", 53.350, -3.141, "125", "Name 3"),
      ("User2", 53.373, -3.143, "220", "Name 2"),
      ("User1", 53.350, -3.141, "130", "Name 1")
    ).toDF("Name", "Lat", "Lng", "Label1", "Label2")

    val groupByCols       = Seq("Name", "Lat", "Lng")
    val aggregateCols     = Seq("Label1", "Label2")
    val defaultMetadata   = ""

    var actualDf = JobGeoMapping.consolidateMetadata(dfIn, groupByCols, aggregateCols, JobGeoMapping.METADATA_SEPARATOR, Some(defaultMetadata))

    // Sort to keep the order (needed for data frame comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,125,130", "Name 1,Name 3,Name 1", 3),
      Row("User2", 53.373, -3.143, "213,220", "Name 2,Name 2", 2)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("consolidateMetadata no default metadata") {

    val dfIn = Seq(
      ("User1", 53.350, -3.141, "120,", "Name 1,"),
      ("User2", 53.373, -3.143, "213", "Name 2"),
      ("User1", 53.350, -3.141, "125", "Name 3"),
      ("User2", 53.373, -3.143, "220", "Name 2"),
      ("User1", 53.350, -3.141, "130", "Name 1")
    ).toDF("Name", "Lat", "Lng", "Label1", "Label2")

    val groupByCols       = Seq("Name", "Lat", "Lng")
    val aggregateCols     = Seq("Label1", "Label2")

    var actualDf = JobGeoMapping.consolidateMetadata(dfIn, groupByCols, aggregateCols, JobGeoMapping.METADATA_SEPARATOR, None)

    // Sort to keep the order (needed for data frame comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 53.350, -3.141, "120,,125,130", "Name 1,,Name 3,Name 1", 4),
      Row("User2", 53.373, -3.143, "213,220", "Name 2,Name 2", 2)
    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }


  test("consolidateMetadata incorrect col names") {

    val dfIn = Seq(
      ("User1", 53.350, -3.141, "120,", "Name 1,"),
      ("User2", 53.373, -3.143, "213", "Name 2"),
      ("User1", 53.350, -3.141, "125", "Name 3"),
      ("User2", 53.373, -3.143, "220", "Name 2"),
      ("User1", 53.350, -3.141, "130", "Name 1")
    ).toDF("Name", "Lat", "Lng", "Label1", "Label2")

    //Incorrect groupByCols
    var groupByCols = Seq("Name11", "Lat", "Lng")
    var aggregateCols = Seq("Label1", "Label2")

    assertThrows[org.apache.spark.sql.AnalysisException] {
      JobGeoMapping.consolidateMetadata(dfIn, groupByCols, aggregateCols, JobGeoMapping.METADATA_SEPARATOR, None)
    }

    //Incorrect aggregateCols
    groupByCols = Seq("Name", "Lat", "Lng")
    aggregateCols = Seq("Label111", "Label2")

    assertThrows[org.apache.spark.sql.AnalysisException] {
      JobGeoMapping.consolidateMetadata(dfIn, groupByCols, aggregateCols, JobGeoMapping.METADATA_SEPARATOR, None)
    }
  }

    test("runMultiPolygonsJob test (Single polygons set)") {

      /* This test  covers
        lat/lon columns
        runMultiPolygonJob works for single polygons set input
        Metadata consolidation
        Magellan index
        No default polygons set
      * * */

      val dfIn = Seq(
        ("User1", 324136.095, 384397.104),
        ("User2", 324011.005, 386869.185),
        ("User3", 325009.696, 386295.830),
        ("User4", 320865.000, 392188.000)       // Outside polygons set but with in default polygons set
      ).toDF("Name", "xCentroid", "yCentroid")

      val xColName           = "xCentroid"
      val yColName           = "yCentroid"
      val polygonsPath       = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
      val metadataToExtract  = Seq(Seq("UID"))
      val magellanIndex      = Some(5)
      val aggregateMetadata  = true
      val polygonCol         = "polygon"

      val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, Seq(polygonsPath), Some(metadataToExtract), Seq(magellanIndex))

      // Run job
      var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToExtract), Seq(magellanIndex), aggregateMetadata, None, None)

      // Sort to keep the order (needed for dataframe comparision)
      actualDf = actualDf.sort("Name")

      val expectedData = Seq(
        Row("User1", 324136.095, 384397.104, 53.350996777067465,  -3.141149882762535, "213", 1),
        Row("User2", 324011.005, 386869.185, 53.373194945386025, -3.1436235641372563, "213", 1),
        Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "120,213", 2),
        Row("User4", 320865.0, 392188.0, 53.4205306818467,-3.1922345095472395,"NoMatch",0  )
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(actualDf.schema)
      ).sort("Name")

      assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
    }

    test("runMultiPolygonsJob test (multi polygons set)") {

      /* This test covers
        lat/lon columns
        runMultiPolygonJob works for multi polygons set
        Metadata consolidation
        Magellan index
        No default polygons set
       */

      val dfIn = Seq(
        ("User1", 324136.095, 384397.104),
        ("User2", 324011.005, 386869.185),
        ("User3", 325009.696, 386295.830),
        ("Dft_User", 320865.000, 392188.000)               // Outside polygons set but with in default polygons set
      ).toDF("Name", "xCentroid", "yCentroid")

      val xColName           = "xCentroid"
      val yColName           = "yCentroid"
      val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
      val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
      val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
      val metadataToExtract  = Seq(Seq("UID"), Seq("Postdist", "Postarea"))
      val magellanIndex      = Seq(None, Some(5))
      val aggregateMetadata  = true
      val polygonCol         = "polygon"
      val toGPS              = true

      val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex)
      var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateMetadata, None, None)

      // Sort to keep the order (needed for dataframe comparision)
      actualDf = actualDf.sort("Name")

      val expectedData = Seq(
        Row("User1", 324136.095, 384397.104, 53.350996777067465, -3.141149882762535, "213", 1, "CH48", "CH", 1),
        Row("User2", 324011.005, 386869.185, 53.373194945386025, -3.1436235641372563, "213", 1, "CH48", "CH", 1),
        Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "120,213", 2, "CH48", "CH", 1),
        Row("Dft_User", 320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch",0,"NoMatch","NoMatch",0)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(actualDf.schema)
      ).sort("Name")

      assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
    }


    test("runMultiPolygonsJob test (no index and no consolidation)") {

      val dfIn = Seq(
        ("User1", 324136.095, 384397.104),
        ("User2", 324011.005, 386869.185),
        ("User3", 325009.696, 386295.83),
        ("Dft_User", 320865.000, 392188.000)               // Outside polygons set but with in default polygons set
      ).toDF("Name", "xCentroid", "yCentroid")

      val xColName           = "xCentroid"
      val yColName           = "yCentroid"
      val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
      val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
      val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
      val metadataToExtract  = Seq(Seq("UID"), Seq("Postdist", "Postarea"))
      val magellanIndex      = Seq(None, None)
      val aggregateMetadata  = false
      val polygonCol         = "polygon"
      val toGPS              = true

      val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex)
      var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateMetadata, None, None)

      // Sort to keep the order (needed for dataframe comparision)
      actualDf = actualDf.sort("Name")

      val expectedData = Seq(
        Row("User1", 324136.095, 384397.104, 53.350996777067465, -3.141149882762535, "213", "CH48", "CH"),
        Row("User2", 324011.005, 386869.185, 53.373194945386025, -3.1436235641372563, "213", "CH48", "CH"),
        Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "120", "CH48", "CH"),
        Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "213", "CH48", "CH"),
        Row("Dft_User",320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch","NoMatch","NoMatch")
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(actualDf.schema)
      ).sort("Name")

      assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
    }

    test("runMultiPolygonsJob test (no GPS)") {
      val dfIn = Seq(
        ("User1", 53.350996777067465, -3.141149882762535),
        ("User2", 53.373194945386025, -3.1436235641372563),
        ("User3", 53.36818516353612, -3.128479626392792),
        ("Dft_User", 53.42053247546133, -3.1922377960725044)  // Outside polygons set but with in default polygons set
      ).toDF("Name", "Lat", "Lon")

      val xColName           = "Lon"    //Point object takes longitude as x
      val yColName           = "Lat"    //Point object takes longitude as y
      val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
      val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
      val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
      val metadataToExtract  = Seq(Seq("UID"), Seq("Postdist", "Postarea"))
      val magellanIndex      = Seq(None, None)
      var aggregateMetadata  = false
      val polygonCol         = "polygon"
      val toGPS              = false

      val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex)
      var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateMetadata, None, None)

      // Sort to keep the order (needed for dataframe comparision)
      actualDf = actualDf.sort("Name")

      // consolidate metadata
      aggregateMetadata  = true
      actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, toGPS, dfMultiPolygons, polygonCol, Some(metadataToExtract), magellanIndex, aggregateMetadata, None, None)

      // Sort to keep the order (needed for dataframe comparision)
      actualDf = actualDf.sort("Name")

      val expectedData = Seq(
        Row("User1", 53.350996777067465, -3.141149882762535, "213", 1, "CH48", "CH", 1),
        Row("User2", 53.373194945386025, -3.1436235641372563, "213", 1, "CH48", "CH", 1),
        Row("User3", 53.36818516353612, -3.128479626392792, "120,213", 2, "CH48", "CH", 1),
        Row("Dft_User",53.42053247546133,-3.1922377960725044,"NoMatch",0,"NoMatch","NoMatch",0)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(actualDf.schema)
      ).sort("Name")

      assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
    }

  test("runMultiPolygonsJob test (default polygons)") {

    val dfIn = Seq(
      ("User1", 324136.095, 384397.104),
      ("User2", 324011.005, 386869.185),
      ("User3", 325009.696, 386295.83),
      ("Dft_User", 320865.000, 392188.000)               // Outside polygons set but with in default polygons set
    ).toDF("Name", "xCentroid", "yCentroid")

    val xColName           = "xCentroid"
    val yColName           = "yCentroid"
    val polygonsPath1      = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val defaultPolygons    = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val polygonsSeq        = Seq(polygonsPath1, polygonsPath2)
    val metadataToExtract  = Seq(Seq("UID"), Seq("Postdist", "Postarea"))
    val magellanIndex      = Seq(None, None)
    val aggregateMetadata  = true
    val polygonCol         = "polygon"
    val toGPS              = true

    // Load polygons
    val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, polygonsSeq, Some(metadataToExtract), magellanIndex)
    val defaultPolygonsDf  = CoordinatesUtils.loadDefaultPolygons(spark, defaultPolygons, Some(metadataToExtract), magellanIndex)
    val unionPolygonsDf    = CoordinatesUtils.unionOfPolygonsDf(dfMultiPolygons, defaultPolygonsDf)

    // Run job
    var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, toGPS, unionPolygonsDf, polygonCol, Some(metadataToExtract), magellanIndex, aggregateMetadata, None, None)

    // Sort to keep the order (needed for dataframe comparision)
    actualDf = actualDf.sort("Name")

    val expectedData = Seq(
      Row("User1", 324136.095, 384397.104, 53.350996777067465, -3.141149882762535, "213,-", 2, "CH48,-", "CH,-", 2),
      Row("User2", 324011.005, 386869.185, 53.373194945386025, -3.1436235641372563, "213,-", 2, "CH48,-", "CH,-", 2),
      Row("User3", 325009.696, 386295.83, 53.36818516353612, -3.128479626392792, "120,213,-", 3, "CH48,-", "CH,-", 2),
      Row("Dft_User", 320865.0, 392188.0, 53.4205306818467, -3.1922345095472395, "-", 1, "-", "-", 1)

    )

    val expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    ).sort("Name")

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  test("CLOpts test default values") {
    val clopts = new JobGeoMapping.CLOpts(Seq(
      "--coords-info", "1::2::PointDataPath.txt"
    ))

    val defaultSeparator          = "\t"
    val defaultMagellanIndex      = ""
    val defaultToGPS              = false
    val defaultPolygonPath        = ""
    val defaultInPartitions       = -1
    val defaultOutPartitions      = -1
    val defaultMetadataToFilter   = ""
    val defaultPolygonsPath       = ""
    val defaultAggregateMetadata  = false

    assert(clopts.separator() === defaultSeparator, "Separator")
    assert(clopts.magellanIndex() === defaultMagellanIndex, "magellan index")
    assert(clopts.coordsInfo() === "1::2::PointDataPath.txt", "coordsInfo")
    assert(clopts.toGPS() === defaultToGPS, "toGPS")
    assert(clopts.polygonsPath() === defaultPolygonPath, "polygonsPath")
    assert(clopts.inPartitions() === defaultInPartitions, "inPartitions")
    assert(clopts.outPartitions() === defaultOutPartitions, "outPartitions")
    assert(clopts.metadataToFilter() === defaultMetadataToFilter, "metadataToFilter")
    assert(clopts.defaultPolygonsPath() === defaultPolygonsPath, "defaultPolygonsPath")
    assert(clopts.aggregateMetadata() === defaultAggregateMetadata, "aggregateMetadata")
  }

  test("CLOpts test mix of default and inputs") {
    val clopts = new JobGeoMapping.CLOpts(Seq(
      "--separator", ",",
      "--magellan-index", "5",
      "--coords-info", "1::2::PointDataPath.txt"
    ))

    val expectedSeparator         = ","
    val expectedMagellanIndex     = "5"
    val defaultToGPS              = false
    val defaultPolygonPath        = ""
    val defaultInPartitions       = -1
    val defaultOutPartitions      = -1
    val defaultMetadataToFilter   = ""
    val defaultPolygonsPath       = ""
    val defaultAggregateMetadata  = false

    assert(clopts.separator() === expectedSeparator, "Separator")
    assert(clopts.magellanIndex() === expectedMagellanIndex, "magellan index")
    assert(clopts.coordsInfo() === "1::2::PointDataPath.txt", "coordsInfo")
    assert(clopts.toGPS() === defaultToGPS, "toGPS")
    assert(clopts.polygonsPath() === defaultPolygonPath, "polygonsPath")
    assert(clopts.inPartitions() === defaultInPartitions, "inPartitions")
    assert(clopts.outPartitions() === defaultOutPartitions, "outPartitions")
    assert(clopts.metadataToFilter() === defaultMetadataToFilter, "metadataToFilter")
    assert(clopts.defaultPolygonsPath() === defaultPolygonsPath, "defaultPolygonsPath")
    assert(clopts.aggregateMetadata() === defaultAggregateMetadata, "aggregateMetadata")
  }

  test("CLOpts test custom validator") {

    var clopts = new JobGeoMapping.CLOpts(Seq(
      "--magellan-index", "5,5,5",
      "--coords-info", "1::2::PointDataPath.txt",
      "--polygons-path", "path1::path2::path3",
      "--metadata-to-filter", "metadata1::metadata2::metadata3"
    ))

    val expectedMagellanIndex = "5,5,5"
    val expectedPolygonsPath = "path1::path2::path3"
    val expectedMetadata = "metadata1::metadata2::metadata3"

    assert(clopts.magellanIndex() === expectedMagellanIndex, "magellan index")
    assert(clopts.polygonsPath() === expectedPolygonsPath, "polygonsPath")
    assert(clopts.metadataToFilter() === expectedMetadata, "metadataToFilter")

    val expectedErrorMessage = "Incorrect arguments: Check magellan index, polygons paths and metadata to filter parameters!!!"
    // Incorrect number of magellan index.
    var argsIn = Array(
      "--magellan-index", "5",
      "--coords-info", "1::2::PointDataPath.txt",
      "--polygons-path", "path1::path2::path3",
      "--metadata-to-filter", "metadata1::metadata2::metadata3"
    )
    TestHelper.clOptsErrorTest(argsIn, expectedErrorMessage)

    // Incorrect number of paths.
    argsIn = Array(
        "--magellan-index", "5,5,5",
        "--coords-info", "1::2::PointDataPath.txt",
        "--polygons-path", "path1::path2",
        "--metadata-to-filter", "metadata1::metadata2::metadata3"
    )
    TestHelper.clOptsErrorTest(argsIn, expectedErrorMessage)

    // Incorrect number of metadata-to-filter.
    argsIn = Array(
      "--magellan-index", "5,5,5",
      "--coords-info", "1::2::PointDataPath.txt",
      "--polygons-path", "path1::path2::path3",
      "--metadata-to-filter", "metadata1::metadata2"
    )
    TestHelper.clOptsErrorTest(argsIn, expectedErrorMessage)

    //Check coordsInfo required
    TestHelper.clOptsErrorTest(Array(""), "Required option 'coords-info' not found")
  }

  test("Multi polygon Output Validator (NoMatch value with Count 0)") {
    /* This test covers below
    1. String 'NoMatch' will be populated in metadata columns of a polygon, if there is no entry found for a source coordinate
    2. At the same time, value 0 will be populated as Count value as there is no match.
   * * */
    val dfIn = Seq(
      ("User1", 324136.095, 384397.104),
      ("User2", 324011.005, 386869.185),
      ("User3", 320865.000, 392188.000)       // Outside polygons set but with in default polygons set
    ).toDF("Name", "xCentroid", "yCentroid")

    val xColName           = "xCentroid"
    val yColName           = "yCentroid"
    val polygonsPath       = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val metadataToExtract  = Seq( Seq("Postdist","Postarea"),Seq("UID","RadioManager","Mobile_Optimiser"))
    val magellanIndex      = Some(5)
    val magellanIndex2      = Some(10)

    val aggregateMetadata  = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, Seq(polygonsPath2,polygonsPath), Some(metadataToExtract), Seq(magellanIndex2,magellanIndex))
    // Run job
    var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToExtract), Seq(magellanIndex,magellanIndex2), aggregateMetadata, None, None)
    val expectedData = Seq(
      Row("User1",324136.095,384397.104,53.350996777067465,-3.141149882762535,"CH48","CH",1,"213","Radio_Mgr2","01234567891",1),
      Row("User2",324011.005,386869.185,53.373194945386025,-3.1436235641372563,"CH48","CH",1,"213","Radio_Mgr2","01234567891",1),
      Row("User3",320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch","NoMatch",0,"NoMatch","NoMatch","NoMatch",0)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    )
    assertDataFrameApproximateEquals(actualDf.sort("name"), expectedDf.sort("name"),000.5)

  }

  test("Multi polygon Output Validator (Populate value '-' when metaddata value is missing or null") {
    /* This test covers below,
    In case there is matching entry in polygon, but metadata is missing or value is null, then metadata default missing value '-' will be populated
   * * */
    val dfIn = Seq(
      ("User1", 324136.095, 384397.104),
      ("User2", 324011.005, 386869.185),
      ("User3", 320865.000, 392188.000),       // Outside polygons set but with in default polygons set
      ("User4", 325009.695, 386295.829)
    ).toDF("Name", "xCentroid", "yCentroid")

    val xColName           = "xCentroid"
    val yColName           = "yCentroid"
    val polygonsPath       = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample_3rows.geojson").getPath
    val polygonsPath2      = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath

    val metadataToExtract  = Seq( Seq("Postdist","Postarea"),Seq("UID","RadioManager","Mobile_Optimiser"))
    val magellanIndex      = Some(5)
    val magellanIndex2      = Some(10)

    val aggregateMetadata  = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = CoordinatesUtils.loadMultiPolygons(spark, Seq(polygonsPath2,polygonsPath), Some(metadataToExtract), Seq(magellanIndex2,magellanIndex))
    // Run job
    var actualDf           = JobGeoMapping.runMultiPolygonJob(spark, dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToExtract), Seq(magellanIndex,magellanIndex2), aggregateMetadata, None, None)
    val expectedData = Seq(
      Row("User1",324136.095,384397.104,53.350996777067465,-3.141149882762535,"CH48","CH",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-",2),
      Row("User2",324011.005,386869.185,53.373194945386025,-3.1436235641372563,"CH48","CH",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-",2),
      Row("User3",320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch","NoMatch",0,"NoMatch","NoMatch","NoMatch",0),
      Row("User4",325009.695,386295.829,53.368185154407186,-3.128479641180844,"CH48","CH",1,"120,213,214","Radio_Mgr1,Radio_Mgr2,Radio_Mgr3","01234567890,-,-",3)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    )
    assertDataFrameApproximateEquals(actualDf.sort("name"), expectedDf.sort("name"),000.5)
  }
}























