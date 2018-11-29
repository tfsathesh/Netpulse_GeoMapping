import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{broadcast, col, collect_list, concat_ws, expr, lit, size, split, udf, when, monotonically_increasing_id}
import org.rogach.scallop._

object JobGeoMapping {

  val MAGELLAN_INDEX_SEPARATOR = ","
  val METADATA_SEPARATOR_1 = "::"
  val METADATA_SEPARATOR = ","
  val POLYGONS_PATH_SEPARATOR = "::"
  // metadata used in default polygons. TODO: This can be optional scallop parameter? need to discuss with reviewer.
  val DEFAULT_POLYGONS_METADATA = ""
  val NOMATCH_IN_POLYGONS = "NoMatch"
  val UNIQUE_ID_COLUMN_NAME="rowid"

  //******* Logging need to be configured via log4j.properties
  //******* CLOpts in different class
  //******* Constanst in separate class
  /** This function builds spark session.
    *
    * @param name        App name
    * @param consoleEcho Flag used to echo spark conf parameters
    * @return Returns spark session.
    */
  def getSparkSession(name: String, consoleEcho: Boolean = true): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(name)
      .config("spark.io.compression.codec", "lz4")
      .config("spark.executor.cores", "3") //********* this needs to be passed as spark submit conf
      .getOrCreate()

    if (consoleEcho)
      spark.conf.getAll.foreach { case (key, value) => println(s"\t$key : $value") }

    spark
  }


  def removeDefaultMetadata(metadataInString: String, metadataToRemove: String, metadataSeparator: String): String = {
    val metadataSeq = metadataInString.split(metadataSeparator)
    if (metadataSeq.length > 1) {
      metadataSeq.filterNot(_ == metadataToRemove).mkString(metadataSeparator)
    }
    else
      metadataSeq.mkString(metadataSeparator)
  }

  /** This function consolidates metadata as list with separator and also adds count column.
    *
    * If default polygons set used as part of points labelling then default metadata exist for each point i.e.
    * Points within the polygons and also points outside polygons will have default metadata. So this function
    * removes the default metadata for the points within the polygons only.
    *
    * @param df                Data frame to consolidate metadata.
    * @param groupByCols       Sequence of columns to be used for group by.
    * @param aggregateCols     Sequence of columns to be used for aggregate operation.
    * @param metadataSeparator Collects metadata as list separated by metadataSeparator
    * @param defaultMetadata   Default metadata string.
    * @return Returns consolidated data frame.
    */
  def consolidateMetadata(
                           df: DataFrame,
                           groupByCols: Seq[String],
                           aggregateCols: Seq[String],
                           metadataSeparator: String,
                           defaultMetadata: Option[String]):
  DataFrame = {
    /** collect_list and concat_ws returns Column type (org.apache.spark.sql.Column).
      * aggCols is a sequence of columns carrying the processing to be applied after the groupBy.
      * */
    val aggCols = aggregateCols.map(colName => expr(s"""concat_ws(\"$metadataSeparator\",collect_list($colName))""").alias(colName))

    var aggregatedDf = df
      .groupBy(groupByCols.map(name => col(name)): _*)
      .agg(aggCols.head, aggCols.tail: _*)

    if (!defaultMetadata.isEmpty) {
      val removeDefaultMetadataUDF = udf { (metadataIn: String) => removeDefaultMetadata(metadataIn, defaultMetadata.get, metadataSeparator) }

      for (colName <- aggregateCols)
        aggregatedDf = aggregatedDf.withColumn(colName, removeDefaultMetadataUDF(col(colName)))
    }

    /** Need to count after removing default metadata.
      * We only need to count one metadata since all metadata count will be same for given polygons set.
      * *
      * Also, need to ignore when the value is "NoMatch" as it means record is not found in Polygon.
      */
    val countIgnoringNull = udf { (concatAggValues: String) => (concatAggValues.split(metadataSeparator).toSeq filter (f => (!f.equals(NOMATCH_IN_POLYGONS)))).length }

    val aggregatedConsolidated = aggregatedDf.withColumn(aggregateCols(0).toString + "_Count", //size(split(col(aggregateCols(0)), metadataSeparator))
      countIgnoringNull(col(aggregateCols(0))))

    aggregatedConsolidated
  }

  case class schema(_c0: Option[String], _c1: Option[String], _c2: Option[String], _c3: Option[String], _c4: Option[String], _c5: Option[String], _c6: Option[String], _c7: Option[String], _c8: Option[String], _c9: Option[String], _c10: Option[String], _c11: Option[String], _c12: Option[String], _c13: Option[String])


  /** This functions runs magellan algorithm for given points and single polygons set.
    *
    * @param df            Points data frame
    * @param xCol          Column name of x centroid or longitude
    * @param yCol          Column name of y centroid or latitude
    * @param toGPS         Coordinates will be converted to WGS84 if toGPS flag is true
    * @param dfPolygons    Polygons set data frame
    * @param polygonCol    Name of the polygons column. Default name is "polygon"
    * @param metadataCols  Metadata to extract from polygons
    * @param magellanIndex Index for magellan algorithm
    * @param outPartitions Out partitions
    * @param outPath       Output path to save results.
    * @return Returns data frame with points labelled.
    */
  def runJob(
              df: DataFrame,
              xCol: String,
              yCol: String,
              toGPS: Boolean = true,
              dfPolygons: DataFrame,
              polygonCol: String = "polygon",
              metadataCols: Option[Seq[String]] = None,
              magellanIndex: Option[Int] = None,
              outPartitions: Option[Int] = None,
              outPath: Option[String] = None
            ): DataFrame = {

    val MAGELLANPOINT_COL = "_magellanPoint"

    var cols = df.columns.toSeq

    val dfIn =
      if (toGPS) {
        cols = cols ++ Seq("lat", "lon")
        CoordinatesUtils.toGPS(df, xCol, yCol)
      }
      else
        df

    if (!metadataCols.isEmpty)
      cols = cols ++ metadataCols.get

    var dfPoints =
      if (toGPS)
        dfIn.withColumn(MAGELLANPOINT_COL, CoordinatesUtils.magellanPointUDF(dfIn.col("lon"), dfIn.col("lat")))
      else
        dfIn.withColumn(MAGELLANPOINT_COL, CoordinatesUtils.magellanPointUDF(dfIn.col(xCol), dfIn.col(yCol)))

    if (!magellanIndex.isEmpty) {
      magellan.Utils.injectRules(df.sparkSession)
      dfPoints = dfPoints.index(magellanIndex.get)
    }

    // Below join is for spark 2.3
    var dfPointsLabeled = broadcast(dfPolygons).join(
      dfPoints, dfPoints.col(MAGELLANPOINT_COL) within dfPolygons.col(polygonCol), "right_outer"
    )
      .select(cols.map(name => col(name)): _*)

    /*
    Below fold left option overrides metadata columns with value 'NoMatch' when there is no match found.
    Here, there is no possibility of having null values in Polygons, as we would have replaced null with '-' before this step.
     */
    var dfPointLabeledWithUnmatch
    = metadataCols.get.foldLeft(dfPointsLabeled) {
      (df, metaColName) =>
        df.withColumn(metaColName,
          when(col(metaColName).isNull, NOMATCH_IN_POLYGONS)
            .otherwise(col(metaColName)))
    }

    if (!outPartitions.isEmpty)
      dfPointsLabeled = dfPointsLabeled.repartition(outPartitions.get)

    //  dfPointLabeledWithUnmatch.show(false)

    if (outPath.isEmpty == false) {
      dfPointLabeledWithUnmatch.write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .csv(outPath.get)
    }

    dfPointLabeledWithUnmatch
  }

  /** This functions runs magellan algorithm for given points and polygons information. Algorithm can be
    * run on either single or multiple polygons. Also it can consolidate multiple metadata information from
    * all the input polygons into a single enriched table.
    *
    * @param spark             Spark Session
    * @param dfIn              Points data frame
    * @param xColName          Column name of x centroid or longitude
    * @param yColName          Column name of y centroid or latitude
    * @param toGPS             Coordinates will be converted to WGS84 if toGPS flag is true
    * @param dfMultiPolygons   Sequence of polygons set data frames
    * @param polygonCol        Name of the polygons column. Default name is "polygon"
    * @param metadataColsSeq   Metadata to extract from polygons
    * @param magellanIndex     Index for magellan algorithm
    * @param aggregateMetadata Consolidates metadata if aggregateMetadata flag is true
    * @param outPartitions     Out partitions
    * @param outPath           Output path to save results.
    * @return Returns data frame with points labelled.
    */
  def runMultiPolygonJob(
                          spark: SparkSession,
                          dfIn: DataFrame,
                          xColName: String,
                          yColName: String,
                          toGPS: Boolean = true,
                          dfMultiPolygons: Seq[DataFrame],
                          polygonCol: String = "polygon",
                          metadataColsSeq: Option[Seq[Seq[String]]] = None,
                          magellanIndex: Seq[Option[Int]] = Seq(None),
                          aggregateMetadata: Boolean = false,
                          outPartitions: Option[Int] = None,
                          outPath: Option[String] = None
                        ): DataFrame = {

    var firstIteration = true
    var inputDf = dfIn
    var xColNameIn = xColName
    var yColNameIn = yColName
    var toGPSIn = toGPS
    var combinedDf = spark.emptyDataFrame
    var groupByCols = dfIn.columns.toSeq
    if (toGPS) {
      groupByCols = groupByCols ++ Seq("lat", "lon")
    }

    for ((dfPolygons, metadataToFilter, index) <- (dfMultiPolygons, metadataColsSeq.get, magellanIndex).zipped.toSeq) {

      val pointsLabeledDf = runJob(inputDf, xColNameIn, yColNameIn, toGPS = toGPSIn, dfPolygons = dfPolygons, metadataCols = Some(metadataToFilter), magellanIndex = index, outPartitions = None, outPath = None)

      val resultDf: DataFrame = if (aggregateMetadata) {
        consolidateMetadata(pointsLabeledDf, groupByCols, metadataToFilter, METADATA_SEPARATOR, Some(DEFAULT_POLYGONS_METADATA))
      } else {
        pointsLabeledDf
      }

      // runJob parameters are different from second iteration if toGPS is true in first iteration.
      if (firstIteration) {
        firstIteration = false
        if (toGPS) {
          xColNameIn = "lon"
          yColNameIn = "lat"
          toGPSIn = false
        }
      }

      // Extra variable used, just for proper in and out naming purpose.
      inputDf = resultDf
      combinedDf = resultDf
      groupByCols = resultDf.columns.toSeq
    }

    combinedDf= combinedDf.drop(UNIQUE_ID_COLUMN_NAME)

    if (!outPartitions.isEmpty)
      combinedDf = combinedDf.repartition(outPartitions.get)

    if (outPath.isEmpty == false) {
      combinedDf.write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .csv(outPath.get)
    }

    // combinedDf.show(false)
    combinedDf
  }

  /** Scallop config parameters
    *
    * @constructor Create a new CLOpts with arguments.
    * @param arguments
    */
  class CLOpts(arguments: Seq[String]) extends ScallopConf(arguments) {
    val separator = opt[String](name = "separator", required = false, default = Some("\t"))
    val magellanIndex = opt[String](name = "magellan-index", required = false, default = Some(""))
    val coordsInfo = opt[String](name = "coords-info", required = true)
    val toGPS = opt[Boolean](name = "to-gps", required = false, default = Some(false))
    val polygonsPath = opt[String](name = "polygons-path", required = false, default = Some(""))
    val inPartitions = opt[Int](name = "in-partitions", required = false, default = Some(-1))
    val outPartitions = opt[Int](name = "out-partitions", required = false, default = Some(-1))
    val metadataToFilter = opt[String](name = "metadata-to-filter", required = false, default = Some(""))
    val defaultPolygonsPath = opt[String](name = "default-polygons-path", required = false, default = Some(""))
    val aggregateMetadata = opt[Boolean](name = "aggregate-labels", required = false, default = Some(true))
    val others = trailArg[List[String]](required = false)
    val loadType = opt[String](name = "load-type", required = false, default = Some("singlejoin"))
    val readOption = opt[String](name = "read-option", required = false, default = Some("DF"))

    validate(magellanIndex, polygonsPath, metadataToFilter) { (index, paths, metadata) =>
      val indexCount = index.split(MAGELLAN_INDEX_SEPARATOR).length
      val pathsCount = paths.split(POLYGONS_PATH_SEPARATOR).length
      val metadataCount = metadata.split(METADATA_SEPARATOR_1).length

      if ((indexCount == pathsCount) && (pathsCount == metadataCount))
        Right(Unit)
      else
        Left("Incorrect arguments: Check magellan index, polygons paths and metadata to filter parameters!!!")
    }

    verify()
  }

  /** This function converts string index to Int magellan index.
    *
    * @param index     Input string index with delimiter.
    * @param separator delimiter to extract index values
    * @return Returns sequence of integer index values.
    */
  def convertStringIndexToIntSeq(index: String, separator: String): Seq[Option[Int]] = {
    val magellanIndex = index.split(separator)

    val retIndex = magellanIndex.map { idx =>
      val magellanIndexInt = if (idx == "" || idx == " ") -1 else idx.toInt
      if (magellanIndexInt > 0 && (magellanIndexInt - magellanIndexInt % 5) >= 5)
        Some(magellanIndexInt - (magellanIndexInt % 5))
      else
        None
    }

    retIndex
  }

  def main(args: Array[String]) {

    val clopts = new CLOpts(args)
    val separator = clopts.separator()
    val inPartitions = clopts.inPartitions()
    val outPartitions = if (clopts.outPartitions() == -1) None else Some(clopts.outPartitions())
    val coordsInfo = clopts.coordsInfo()
    val toGPS = clopts.toGPS()
    val polygonsPath = if (clopts.polygonsPath() == "") None else Some(clopts.polygonsPath().split(POLYGONS_PATH_SEPARATOR).toSeq)
    val metadataToFilter = if (clopts.metadataToFilter() == "") None else Some(clopts.metadataToFilter().split(METADATA_SEPARATOR_1).toSeq)
    val others = clopts.others()
    val outPath = others(0)
    val magellanIndex = convertStringIndexToIntSeq(clopts.magellanIndex(), MAGELLAN_INDEX_SEPARATOR)
    val defaultPolygonsPath = if (clopts.defaultPolygonsPath() == "") None else Some(clopts.defaultPolygonsPath())
    val aggregateMetadata = clopts.aggregateMetadata()
    val loadType = clopts.loadType()
    val readOption = clopts.readOption()
    // metadataToFilter type is Option[Seq[String]]. Each element is separated by ",".
    // This need to be converted to Seq[Seq[String]]
    val metadataToFilterSeq: Seq[Seq[String]] = for {
      metaData <- metadataToFilter.getOrElse(Seq(""))
    } yield metaData.split(METADATA_SEPARATOR).toSeq

    val tmp = coordsInfo.split("::")
    val xColName = "_c%s".format(tmp(0)) //_c12   //********* We may need to cast this manually
    val yColName = "_c%s".format(tmp(1)) //_c13   //********* We may need to cast this manually
    val coordsPath = tmp(2)

    val spark = getSparkSession("JobGeoMapping")

    import spark.implicits._

    var dfIn = spark.sparkContext.parallelize(List("10")).toDF()

    if (readOption == "DF") {
      dfIn = spark
        .read
        .format("com.databricks.spark.csv")
        .option("delimiter", separator)
        .option("header", "false")
        .csv(coordsPath)
    }
    else {
      val rddIn = spark
        .read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .csv(coordsPath).rdd
      dfIn = rddIn.map(_.mkString.split(separator)).map(x => schema(x.lift(0), x.lift(1), x.lift(2), x.lift(3), x.lift(4), x.lift(5), x.lift(6), x.lift(7), x.lift(8), x.lift(9), x.lift(10), x.lift(11), x.lift(12), x.lift(13))).toDF()
    }

    val dfWithRowId = dfIn.withColumn(UNIQUE_ID_COLUMN_NAME,monotonically_increasing_id())

    val dfIn2 =
      if (inPartitions > 0)
        dfWithRowId.repartition(inPartitions)
      else
        dfWithRowId

    // dfIn2.show(false)

    if (loadType != "multijoin") {
      println("*********************************************** SingleJoin Run **********************************" + separator)
      var polygonsUnionSet = PolygonsUtils.loadPolygonsSet(spark, polygonsPath.get, Some(metadataToFilterSeq), magellanIndex)
      //******* Need to Handle default polygons Path scenario
      PolygonsUtils.runPolygonsUnionJob(spark, dfIn2, xColName, yColName, toGPS, polygonsUnionSet, "polygon", Some(metadataToFilterSeq), magellanIndex, aggregateMetadata, outPartitions = outPartitions, outPath = Some(outPath))
    }
    else {
      println("*********************************************** MultiJoin Run **********************************" + separator)
      var polygonsDfSeq = CoordinatesUtils.loadMultiPolygons(spark, polygonsPath.get, Some(metadataToFilterSeq), magellanIndex)
      if (!defaultPolygonsPath.isEmpty) {
        val defaultPolygonsDfSeq = CoordinatesUtils.loadDefaultPolygons(spark, defaultPolygonsPath.get, Some(metadataToFilterSeq), magellanIndex)
        polygonsDfSeq = CoordinatesUtils.unionOfPolygonsDf(polygonsDfSeq, defaultPolygonsDfSeq)
      }
      runMultiPolygonJob(spark, dfIn2, xColName, yColName, toGPS, polygonsDfSeq, "polygon", Some(metadataToFilterSeq), magellanIndex, aggregateMetadata, outPartitions = outPartitions, outPath = Some(outPath))

    }


  }
}