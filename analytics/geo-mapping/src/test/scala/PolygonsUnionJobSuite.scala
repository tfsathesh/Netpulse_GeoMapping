import JobGeoMapping.{DEFAULT_POLYGONS_METADATA, METADATA_SEPARATOR, runMultiPolygonJob}
import PolygonsUnionJob.consolidateUnionMetadata
import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.{col, expr, lit, udf, when}



class PolygonsUnionJobSuite extends FunSuite with DataFrameSuiteBase {

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

    val metadataToExtract  = Seq( Seq("Postdist_0","Postarea_0"),Seq("UID_1","RadioManager_1","Mobile_Optimiser_1"))
    val magellanIndex      = Some(5)
    val magellanIndex2      = Some(10)
    val metadataList = metadataToExtract.foldLeft(Seq(""))(_++_).tail
    val aggregateMetadata  = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = PolygonsUnionJob.loadPolygonsSet(spark, Seq(polygonsPath2,polygonsPath), Some(metadataToExtract), Seq(magellanIndex2,magellanIndex))
    var actualDf           = PolygonsUnionJob.runPolygonsUnionJoinJob(spark, dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToExtract), Seq(magellanIndex,magellanIndex2), aggregateMetadata, None, None)

    actualDf = actualDf.sort((Seq("Name")++metadataList).map(c => col(c)):_*)
    actualDf = sortDFMetadata(actualDf,metadataList)

    val expectedData = Seq(
      Row("User1",324136.095,384397.104,53.350996777067465,-3.141149882762535,"CH48","CH",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-",2),
      Row("User2",324011.005,386869.185,53.373194945386025,-3.1436235641372563,"CH48","CH",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-",2),
      Row("User3",320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch","NoMatch",0,"NoMatch","NoMatch","NoMatch",0),
      Row("User4",325009.695,386295.829,53.368185154407186,-3.128479641180844,"CH48","CH",1,"120,213,214","Radio_Mgr1,Radio_Mgr2,Radio_Mgr3","-,-,01234567890",3)
    )

    var expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    ).sort((Seq("Name")++metadataList).map(c=> col(c)):_*)

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }

  def sortDFMetadata(df: DataFrame, metadataList:Seq[String]):DataFrame= {
    val sortUDF = udf{(UID: String) => UID.split(",").sorted.mkString(",")}
    val cols = df.columns.toSeq
    df.select(cols.map(c=>
      (if (metadataList.contains(c)) sortUDF(col(c))
      else col(c)).alias(c)
    )
      :_*)
  }


  test("Multi polygon load Validator (for similar metadata column in more than one Polygon)") {
    // Test case to validate scenario where similar metadata column(REGION) name exists in more than one poligon
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

    val metadataToExtract  = Seq( Seq("Postdist_0","Postarea_0","REGION_0"),Seq("UID_1","RadioManager_1","Mobile_Optimiser_1","REGION_1"))
    val magellanIndex      = Some(5)
    val magellanIndex2      = Some(10)
    val metadataList = metadataToExtract.foldLeft(Seq(""))(_++_).tail
    val aggregateMetadata  = true
    val polygonCol         = "polygon"

    val dfMultiPolygons    = PolygonsUnionJob.loadPolygonsSet(spark, Seq(polygonsPath2,polygonsPath), Some(metadataToExtract), Seq(magellanIndex2,magellanIndex))
    var actualDf           = PolygonsUnionJob.runPolygonsUnionJoinJob(spark, dfIn, xColName, yColName, true, dfMultiPolygons, polygonCol, Some(metadataToExtract), Seq(magellanIndex,magellanIndex2), aggregateMetadata, None, None)
    actualDf = sortDFMetadata(actualDf,metadataList)
    actualDf = actualDf.sort((Seq("Name")++metadataList).map(c => col(c)):_*)


    val expectedData = Seq(
      Row("User1",324136.095,384397.104,53.350996777067465,-3.141149882762535,"CH48","CH","region1",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-","region2,region2",2),
      Row("User2",324011.005,386869.185,53.373194945386025,-3.1436235641372563,"CH48","CH","region1",1,"213,214","Radio_Mgr2,Radio_Mgr3","-,-","region2,region2",2),
      Row("User3",320865.0,392188.0,53.4205306818467,-3.1922345095472395,"NoMatch","NoMatch","NoMatch",0,"NoMatch","NoMatch","NoMatch","NoMatch",0),
      Row("User4",325009.695,386295.829,53.368185154407186,-3.128479641180844,"CH48","CH","region1",1,"120,213,214","Radio_Mgr1,Radio_Mgr2,Radio_Mgr3","-,-,01234567890","region1,region2,region2",3)
    )

    var expectedDF = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(actualDf.schema)
    ).sort((Seq("Name")++metadataList).map(c=> col(c)):_*)

    assertDataFrameApproximateEquals(expectedDF, actualDf, 0.005)
  }


  test("consolidateMetadata single metadata field(Polygons Union Join") {
    // This test case is to verify aggreation in single Join Run with multi Polygons Union
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

    val metadataColsSeq : Option[Seq[Seq[String]]] = Option(Seq(Seq("Label1")))
    val metadataColsFullList = metadataColsSeq.get.foldLeft(Seq(""))(_++_)

    var actualDf = PolygonsUnionJob.consolidateUnionMetadata(dfIn, groupByCols, metadataColsSeq, metadataColsFullList, METADATA_SEPARATOR, Some(DEFAULT_POLYGONS_METADATA))
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
}
