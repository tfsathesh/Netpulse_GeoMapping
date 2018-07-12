import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession

class CoordinatesUtilsSuite extends FunSuite with SharedSparkContext with DataFrameSuiteBase {
  override implicit def reuseContextIfPossible: Boolean = true
  /**
    *
    *
    * @param
    * @return
    */
  def loadMultiPolygonsTest(spark:SparkSession,
                            pathSeq:Seq[String],
                            metadataToExtractSeq:Option[Seq[Seq[String]]] = None,
                            magellanIndex:Seq[Option[Int]] = Seq(None)
                           ): Unit = {

    assert(metadataToExtractSeq.get.length === magellanIndex.length)
    assert(pathSeq.length === magellanIndex.length)

    val expectedPolygonSetCount = pathSeq.length
    // Load with out default set
    val loadedPolygonsDf = CoordinatesUtils.loadMultiPolygons(spark, pathSeq, metadataToExtractSeq, magellanIndex)

    assert(loadedPolygonsDf.length === expectedPolygonSetCount)

    // Verify polygons loaded and dataframe contains metadata columns.
    for ((polygonsDf, metadataToExtract, index) <- (loadedPolygonsDf, metadataToExtractSeq.get, magellanIndex).zipped.toSeq) {
      assertTrue(polygonsDf.count >= 1)
      assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToExtract))
      if(!index.isEmpty)
        assertTrue(polygonsDf.columns.seq.contains("index"))
      else
        assertTrue(!polygonsDf.columns.seq.contains("index"))
    }
  }

  def loadDefaultPolygonsTest(spark:SparkSession,
                            path:String,
                            metadataToExtractSeq:Option[Seq[Seq[String]]] = None,
                            magellanIndex:Seq[Option[Int]] = Seq(None)
                           ): Unit = {

    assert(metadataToExtractSeq.get.length === magellanIndex.length)

    val expectedPolygonSetCount = magellanIndex.length
    // Load with out default set
    val loadedPolygonsDf = CoordinatesUtils.loadDefaultPolygons(spark, path, metadataToExtractSeq, magellanIndex)

    assert(loadedPolygonsDf.length === expectedPolygonSetCount)

    // Verify polygons loaded and dataframe contains metadata columns.
    for ((polygonsDf, metadataToExtract) <- loadedPolygonsDf.zip(metadataToExtractSeq.get)) {
      assertTrue(polygonsDf.count >= 1)
      assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToExtract))
    }
  }

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
    //assertTrue(polygonsDf.select(polygonsDf("Postdist1")).collect().forall(_ === "-"))

    // Check correct metadata value is not "-"
    //TODO
    //assertTrue(polygonsDf.select(polygonsDf("Postarea")).collect().forall(_ != "-"))
  }


  test("loadMultiPolygons test 1") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                          Seq("Postdist", "Postarea"),
                                          Seq("name"))
    val magellanIndex                   = Seq(Some(5), None, Some(15))

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons test 2 (single path)") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID", "Optimiser", "CHUNK,AREA_OP", "AREA_OWNER"))
    val magellanIndex                   = Seq(Some(5))

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons test 4") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1)
    val metadataToExtractSeq            = Seq(Seq(""))
    val magellanIndex                   = Seq(Some(5))

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadMultiPolygons test 5") {

    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val multiPolygonsPath               = Seq(polygonsPath1, polygonsPath2, polygonsPath3)
    val metadataToExtractSeq            = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
      Seq("Postdist", "Postarea"),
      Seq("name"))
    val magellanIndex                   = Seq(None, None, None)

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(metadataToExtractSeq), magellanIndex)
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

    loadMultiPolygonsTest(spark, multiPolygonsPath, Some(multiPolygonsMetadataToExtract), magellanIndex)
  }

  test("loadDefaultPolygons test 1") {
    val defaultpolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val metadataToExtractSeq              = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                          Seq("Postdist", "Postarea"),
                                          Seq("name"))
    val magellanIndex                   = Seq(Some(10), Some(5), None)

    loadDefaultPolygonsTest(spark, defaultpolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadDefaultPolygons test 2") {
    val defaultpolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val metadataToExtractSeq              = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"),
                                            Seq("Postdist", "Postarea"))
    val magellanIndex                   = Seq(Some(10), None)

    loadDefaultPolygonsTest(spark, defaultpolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("loadDefaultPolygons test 3") {
    val defaultpolygonsPath               = this.getClass.getClassLoader.getResource("geojson/defaultPolygon.geojson").getPath
    val metadataToExtractSeq              = Seq(Seq("RadioManager", "UID" ,"Optimiser", "CHUNK", "AREA_OP", "AREA_OWNER"))
    val magellanIndex                   = Seq(Some(10))

    loadDefaultPolygonsTest(spark, defaultpolygonsPath, Some(metadataToExtractSeq), magellanIndex)
  }

  test("unionOfPolygonsDf test1") {
    val polygonsPath1                   = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val polygonsPath2                   = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val polygonsPath3                   = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath

    val polygonsPathSeq                 = Seq(polygonsPath1, polygonsPath2, polygonsPath1)
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
}
