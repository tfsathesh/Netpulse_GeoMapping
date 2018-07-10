import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
/*
class CoordinatesUtilsSuite extends FunSuite with SharedSparkContext with DataFrameSuiteBase {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  test("loadPolygons test 1") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/beacon_gps_sample.geojson").getPath
    val metadataToFilter = "RadioManager,UID,Optimiser,CHUNK,AREA_OP,AREA_OWNER".split(",")
    val magellanIndex = 10

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), Some(magellanIndex))

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))
  }

  test("loadPolygons test 2") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val metadataToFilter = "Postdist,Postarea".split(",")

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), None)

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))

    // Does not contain index column, which is only created when magellan index parameter is not None
    assertTrue(!polygonsDf.columns.toSeq.contains("index"))
  }

  test("loadPolygons test 3") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val metadataToFilter = "name".split(",")
    val magellanIndex = 15

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), Some(magellanIndex))

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))
    assertTrue(polygonsDf.columns.toSeq.contains("index"))
  }

  test("loadPolygons test 4 (metadata None)") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/districts_gps_sample.geojson").getPath
    val metadataToFilter = "name".split(",")
    val magellanIndex = 15

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, None, Some(magellanIndex))
    assert(polygonsDf.count >= 1)
    assertTrue(!polygonsDf.columns.toSeq.containsSlice(metadataToFilter))
  }

  test("loadPolygons test 5 (path does not exists)") {

    val polygonsPath = "/Path/DoesNotExists/"
    val metadataToFilter = "name".split(",")
    val magellanIndex = 15

    assertThrows[org.apache.hadoop.mapreduce.lib.input.InvalidInputException] {
      val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), Some(magellanIndex))
      assert(polygonsDf.count >= 1)
    }
  }

  test("loadPolygons test 6 (Incorrect metadata)") {

    val polygonsPath = this.getClass.getClassLoader.getResource("geojson/postdist_gps_sample.geojson").getPath
    val metadataToFilter = "Postdist1,Postarea".split(",")

    val polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), None)

    assert(polygonsDf.count >= 1)
    assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToFilter))

    // Check incorrect metadata value is "-"
    //TODO

    // Check correct metadata value is not "-"
    //TODO
  }
}
*/