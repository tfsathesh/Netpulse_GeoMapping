import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import org.apache.spark.sql.SparkSession
import org.rogach.scallop.exceptions.ScallopException

object TestHelper extends FunSuite with DataFrameSuiteBase {

  /**
    *
    *
    * @param
    * @return
    */
  def clOptsErrorTest(args: Array[String], expectedErrorMessage: String): Unit ={
    val clopts = new JobGeoMapping.CLOpts(args) {
      override def onError(e: Throwable): Unit = e match {
        case ScallopException(message) => assert(message === expectedErrorMessage)
        case _ => fail("Unexpected error !!!")
      }
    }
  }

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

    // This count will represent number of polygons.
    val expectedPolygonSetCount = pathSeq.length

    val loadedPolygonsDf = CoordinatesUtils.loadMultiPolygons(spark, pathSeq, metadataToExtractSeq, magellanIndex)

    assert(loadedPolygonsDf.length === expectedPolygonSetCount)

    //
    val metadataSeqIn:Seq[Seq[String]] = if(metadataToExtractSeq.isEmpty)
      Seq.fill(pathSeq.length)(Seq(""))
    else
      metadataToExtractSeq.get

    //
    val indexSeqIn:Seq[Option[Int]] = if(magellanIndex.length <= 1 && magellanIndex(0).isEmpty)
      Seq.fill(pathSeq.length)(None)
    else
      magellanIndex


    // Verify polygons loaded and dataframe contains metadata columns.
    for ((polygonsDf, metadataToExtract, index) <- (loadedPolygonsDf, metadataSeqIn, indexSeqIn).zipped.toSeq) {
      assertTrue(polygonsDf.count >= 1)
      assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToExtract))

      if(!index.isEmpty)
        assertTrue(polygonsDf.columns.seq.contains("index"))
      else
        assertTrue(!polygonsDf.columns.seq.contains("index"))
    }

    //TODO: do we need to check data frame content??
  }

  /**
    *
    *
    * @param
    * @return
    */
  def loadDefaultPolygonsTest(spark:SparkSession,
                              path:String,
                              metadataToExtractSeq:Option[Seq[Seq[String]]] = None,
                              magellanIndex:Seq[Option[Int]] = Seq(None)
                             ): Unit = {

    // This count will represent number of polygons.
    val expectedPolygonSetCount = magellanIndex.length
    // Load default set
    val loadedPolygonsDf = CoordinatesUtils.loadDefaultPolygons(spark, path, metadataToExtractSeq, magellanIndex)

    assert(loadedPolygonsDf.length === expectedPolygonSetCount)

    // Verify polygons loaded and dataframe contains metadata columns.
    for ((polygonsDf, metadataToExtract, index) <- (loadedPolygonsDf, metadataToExtractSeq.getOrElse(Seq(Seq(""))), magellanIndex).zipped.toSeq) {
      assertTrue(polygonsDf.count >= 1)
      assertTrue(polygonsDf.columns.toSeq.containsSlice(metadataToExtract))

      if(!index.isEmpty)
        assertTrue(polygonsDf.columns.seq.contains("index"))
      else
        assertTrue(!polygonsDf.columns.seq.contains("index"))
    }

    //TODO: do we need to check data frame content??
  }

}
