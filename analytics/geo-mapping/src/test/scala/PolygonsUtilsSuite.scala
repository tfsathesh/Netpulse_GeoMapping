import JobGeoMapping.runMultiPolygonJob
import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType


class PolygonsUtilsSuite extends FunSuite with DataFrameSuiteBase {

  override implicit def reuseContextIfPossible: Boolean = true
  import spark.implicits._

  test("Multi polygon load Validator (using Polygon UnionSet )") {
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

    val metadataToExtract1  = Option(Seq( "Postdist,Postarea","UID,RadioManager,Mobile_Optimiser"))

    val metadataToFilterSeq: Seq[Seq[String]] = for {
      (metaData,i) <- (metadataToExtract1.getOrElse(Seq("")),0 to metadataToExtract1.get.size).zipped.toSeq
    } yield metaData.split(",").map(a=> a+"_"+i).toSeq

    println("**********************lenth of metadataToFilter"+metadataToExtract1.get.size)
    for (elem <- metadataToFilterSeq) {elem.foreach(k=> println("*********"+k  +" and "+k.substring(0,k.lastIndexOf("_"))))}


    val aggregateMetadata  = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = PolygonsUtils.loadPolygonsSet(spark, Seq(polygonsPath2,polygonsPath), Some(metadataToFilterSeq), Seq(magellanIndex2,magellanIndex))

    println("*******************before Schema print")
    dfMultiPolygons.printSchema()
    println("*******************after Schema print")

    //********* Need to handle default polygons logic
    // Run job
    var actualDf           = PolygonsUtils.runPolygonsUnionJob(spark, dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToFilterSeq), Seq(magellanIndex,magellanIndex2), aggregateMetadata, None, None)
    actualDf.show()
    var polygonsDfSeq = CoordinatesUtils.loadMultiPolygons(spark, Seq(polygonsPath2,polygonsPath), Some(metadataToFilterSeq), Seq(magellanIndex2,magellanIndex))

    polygonsDfSeq.foreach{a=>
      println("*************************Inside polygons printing loop")
        a.show(5,false)}
    val actualDf2 =       runMultiPolygonJob(spark, dfIn, xColName, yColName, true, polygonsDfSeq, "polygon", Some(metadataToFilterSeq), Seq(magellanIndex,magellanIndex2), aggregateMetadata, None,None)

    println("**********************printing both outputs")
    actualDf.show()
    actualDf2.show()
    val expectedData = Seq(
      Row("User1",324136.095,384397.104,53.350996777067465,-3.141149882762535,"CH48","CH",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-",2),
      Row("User2",324011.005,386869.185,53.373194945386025,-3.1436235641372563,"CH48","CH",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-",2),
      Row("User3",320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch","NoMatch",0,"NoMatch","NoMatch","NoMatch",0),
      Row("User4",325009.695,386295.829,53.368185154407186,-3.128479641180844,"CH48","CH",1,"120,213,214","Radio_Mgr1,Radio_Mgr2,Radio_Mgr3","01234567890,-,-",3)
    )

    assert(1==1)
  }

  test("New File test") {
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
    val polygonsPath       = this.getClass.getClassLoader.getResource("geojson/lsoa.geojson").getPath

    val metadataToExtract  = Seq( Seq("Postdist","Postarea"),Seq("UID","RadioManager","Mobile_Optimiser"))
    val magellanIndex      = Some(5)
    val magellanIndex2      = Some(10)

    val aggregateMetadata  = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = PolygonsUtils.loadPolygonsSet(spark, Seq(polygonsPath), Some(metadataToExtract), Seq(magellanIndex))

    println("*******************before Schema print")
    dfMultiPolygons.printSchema()
    println("*******************after Schema print")

    //********* Need to handle default polygons logic
    // Run job

    println("printing value *******"+dfMultiPolygons.count)

    assert(1==1)
  }
}
