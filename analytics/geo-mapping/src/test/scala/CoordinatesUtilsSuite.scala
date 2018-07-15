import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.StructType

class CoordinatesUtilsSuite extends FunSuite with DataFrameSuiteBase {
  override implicit def reuseContextIfPossible: Boolean = true
  import spark.implicits._

  test("loadPolygons test 0 ") {
    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val magellanIndex = 10

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, None, Some(magellanIndex))

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.contains("index"))

    val expectedDf = spark.read
      .format("magellan")
      .option("type", "geojson")
      .option("magellan.index", "true")
      .option("magellan.index.precision", magellanIndex.toString)
      .load(polygonsPath)

    assertDataFrameApproximateEquals(expectedDf, polygonsDf, 0.005)
  }

  test("loadPolygons test 1") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val metadataToFilter = Seq("RadioManager", "UID", "Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER")
    val magellanIndex = 10

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), Some(magellanIndex))

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))
    assertTrue(polygonsDf.columns.toSeq.contains("index"))
  }

  test("loadPolygons test 2") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val metadataToFilter = Seq("Postdist", "Postarea")

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), None)

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))

    // Does not contain index column, which is only created when magellan index parameter is not None
    assertTrue(!polygonsDf.columns.toSeq.contains("index"))
  }

  test("loadPolygons test 3") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val metadataToFilter = Seq("name")
    val magellanIndex = 15

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), Some(magellanIndex))

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))
    assertTrue(polygonsDf.columns.toSeq.contains("index"))

  }

  test("loadPolygons test 4 (metadata None)") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val metadataToFilter = Seq("name")
    val magellanIndex = 15

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, None, Some(magellanIndex))
    assert(polygonsDf.count >= 1)
    assertTrue(!polygonsDf.columns.toSeq.containsSlice(metadataToFilter))
    assertTrue(polygonsDf.columns.toSeq.contains("index"))
  }

  test("loadPolygons test 5 (path does not exists)") {

    val polygonsPath = "/Path/DoesNotExists/"
    val metadataToFilter = Seq("name")
    val magellanIndex = 15

    assertThrows[org.apache.hadoop.mapreduce.lib.input.InvalidInputException] {
      val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), Some(magellanIndex))
      assert(polygonsDf.count >= 1)
    }
  }

  test("loadPolygons test 6 (Incorrect metadata)") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val metadataToFilter = Seq("Postdist1", "Postarea")

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), None)

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))

    // Check incorrect metadata value is "-"
    //TODO
    //assertTrue(polygonsDf.select(polygonsDf("Postdist1")).collect.forall(_ === "-"))

    // Check correct metadata value is not "-"
    //TODO
    //assertTrue(polygonsDf.select(polygonsDf("Postarea")).collect.forall(_ != "-"))
  }


  test("loadMultiPolygons test") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                          Seq("Postdist", "Postarea"),
                                          Seq("name"))
    val magellanIndex                   = Seq(Some(5), None, Some(15))

    TestHelper.loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons (single path)") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID", "Optimiser", "CHUNK,AREA_OP", "AREA_OWNER"))
    val magellanIndex                   = Seq(Some(5))

    TestHelper.loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons empty metadata") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1)
    val metadataToExtractSeq            = Seq(Seq(""))
    val magellanIndex                   = Seq(Some(5))

    TestHelper.loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons test magellan index none") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
      Seq("Postdist", "Postarea"),
      Seq("name"))
    val magellanIndex                   = Seq(None, None, None)

    TestHelper.loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons test default parameters") {
    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)

    TestHelper.loadMultiPolygonsTest(spark, multiPolygonsPath)
  }

  ignore("loadMultiPolygons test with shp files") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("shapefiles/beacon_gps_sample/").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("shapefiles/postdist_gps_sample/").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("shapefiles/districts_gps_sample/").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val multiPolygonsMetadataToExtract  = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                          Seq("Postdist", "Postarea"),
                                          Seq("name"))
    val magellanIndex                   = Seq(Some(10), Some(5), None)

    TestHelper.loadMultiPolygonsTest(spark, multiPolygonsPath, Some(multiPolygonsMetadataToExtract), magellanIndex)
  }

  test("loadDefaultPolygons test1") {
    val defaultPolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val metadataToExtractSeq              = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                          Seq("Postdist", "Postarea"),
                                          Seq("name"))
    val magellanIndex                   = Seq(Some(10), Some(5), None)

    TestHelper.loadDefaultPolygonsTest(spark, defaultPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadDefaultPolygons test2") {
    val defaultPolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val metadataToExtractSeq              = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                            Seq("Postdist", "Postarea"))
    val magellanIndex                   = Seq(Some(10), None)

    TestHelper.loadDefaultPolygonsTest(spark, defaultPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadDefaultPolygons with single metadata") {
    val defaultPolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val metadataToExtractSeq              = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"))
    val magellanIndex                   = Seq(Some(10))

    TestHelper.loadDefaultPolygonsTest(spark, defaultPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadDefaultPolygons test default parameters") {
    val defaultPolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath

    TestHelper.loadDefaultPolygonsTest(spark, defaultPolygonsPath)
  }

  test("unionOfPolygonsDf test1") {
    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath

    val polygonsPathSeq                 = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                          Seq("Postdist", "Postarea"),
                                          Seq("name"))

    val magellanIndex                   = Seq(Some(10), Some(5), None)

    val defaultPolygonsPath             = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath

    val polygonsDfSeq = CoordinatesUtils.loadMultiPolygons(spark, polygonsPathSeq, Some(metadataToExtractSeq), magellanIndex)
    val defaultPolygonsDfSeq = CoordinatesUtils.loadDefaultPolygons(spark, defaultPolygonsPath, Some(metadataToExtractSeq), magellanIndex)

    val actualDfSeq = CoordinatesUtils.unionOfPolygonsDf(polygonsDfSeq, defaultPolygonsDfSeq)

    assert(polygonsDfSeq.length === defaultPolygonsDfSeq.length)
    assert(polygonsDfSeq.length === actualDfSeq.length)

    for((realPolygonsDf, defaultPolygonsDf, actualDf) <- (polygonsDfSeq, defaultPolygonsDfSeq, actualDfSeq).zipped.toSeq) {
      assert(realPolygonsDf.schema === defaultPolygonsDf.schema)
      assert(actualDf.schema === defaultPolygonsDf.schema)

      val expectedDf = realPolygonsDf.union(defaultPolygonsDf)

      assertDataFrameApproximateEquals(expectedDf, actualDf, 0.005)
    }
  }

  test("toGPS test conversion") {
    //def toGPS(df:DataFrame, xCol:String, yCol:String) : DataFrame = {
    val dfIn = Seq(
      (324136.095, 384397.104),
      (324011.005, 386869.185),
      (322813.033, 384881.837),
      (322333.396, 386497.332),
      (322834.232, 384312.657),
      (322063.609, 387763.394),
      (322335.586, 385372.306),
      (331362.605, 380615.023),
      (331066.549, 392061.442),
      (327356.172, 390689.014),
      (325596.000, 387311.000)
    ).toDF("xCentroid", "yCentroid")

    val actualDf = CoordinatesUtils.toGPS(dfIn, "xCentroid", "yCentroid")

    val expectedData = Seq(
      Row(324136.095, 384397.104, 53.35099678, -3.141149883),
      Row(324011.005, 386869.185, 53.37319495, -3.143623564),
      Row(322813.033, 384881.837, 53.35516125, -3.161139938),
      Row(322333.396, 386497.332, 53.36960895, -3.168741824),
      Row(322834.232, 384312.657, 53.35004925, -3.160682502),
      Row(322063.609, 387763.394, 53.38094691, -3.173108327),
      Row(322335.586, 385372.306, 53.35949896, -3.168432186),
      Row(331362.605, 380615.023, 53.31799526238038, -3.0317870546147434),
      Row(331066.549, 392061.442, 53.42082716988047, -3.0387306574442197),
      Row(327356.172, 390689.014, 53.407994633513766, -3.0942343205408784),
      Row(325596.000, 387311.000, 53.37739135826915,  -3.1199093810384695)
    )

    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema))

    assertDataFrameApproximateEquals(expectedDf, actualDf, 0.005)
  }
}
