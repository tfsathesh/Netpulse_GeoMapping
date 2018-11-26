import JobGeoMapping._
import org.apache.hadoop.hive.serde2.objectinspector.StructField
import org.apache.spark.sql.functions._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.dmg.pmml.False
object PolygonsUtils {

  val NOMATCH_IN_POLYGONS = "NoMatch"
  val NOT_RELATED_COLUMN = "IgnoreColumn"


  def loadPolygonsSet(spark: SparkSession,
                      pathSeq: Seq[String],
                      metadataToExtractSeq: Option[Seq[Seq[String]]] = None,
                      indexSeq: Seq[Option[Int]] = Seq(None)
                     ): DataFrame = {

    var polygonsDfs = Seq[DataFrame]()
    import spark.implicits._
    var polygonsUnionDf: DataFrame = Seq.empty[(String)].toDF("k")  //creating dummy df, which will be over written later
    var allPolygonsColumns = Set()


    // If metadata is default values then convert it to Seq of defaults based on number of polygons sets.
    val metadataSeqIn: Seq[Seq[String]] = if (metadataToExtractSeq.isEmpty)
      Seq.fill(pathSeq.length)(Seq(""))
    else
      metadataToExtractSeq.get

    // If index is default values then convert it to Seq of defaults based on number of polygons sets.
    val indexSeqIn: Seq[Option[Int]] = if (indexSeq.length == 1 && indexSeq(0).isEmpty)
      Seq.fill(pathSeq.length)(None)
    else
      indexSeq

    var polygonIndex = 0

    def expr(dfCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map(c =>
        c match {
          case c if dfCols.contains(c) => col(c)
          case _ => lit(NOT_RELATED_COLUMN).as(c)
        })
    }
    //
    for ((polygonsPath, metadataToExtract, index) <- (pathSeq, metadataSeqIn, indexSeqIn).zipped.toSeq) {
      val df = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToExtract), index)

      if (polygonIndex.equals(0)) {
        polygonsUnionDf = df
      } else {
        val cols1 = polygonsUnionDf.columns.toSet
        val cols2 = df.columns.toSet
        val total = cols1 ++ cols2
        polygonsUnionDf = polygonsUnionDf.select(expr(cols1, total): _*).union(df.select(expr(cols2, total): _*))
      }

      polygonIndex = polygonIndex + 1 // increment index

    }
    polygonsUnionDf
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
  def runPolygonsUnionJob(
                           spark: SparkSession,
                           dfIn: DataFrame,
                           xColName: String,
                           yColName: String,
                           toGPS: Boolean = true,
                           dfMultiPolygons: DataFrame,
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

    val MAGELLANPOINT_COL = "_magellanPoint"

    var cols = inputDf.columns.toSeq
    val xCol = xColNameIn
    val yCol = yColNameIn
    var metadataColsFullList= Seq[String]()

    inputDf =
      if (toGPS) {
        cols = cols ++ Seq("lat", "lon")
        CoordinatesUtils.toGPS(inputDf, xCol, yCol)
      }
      else
        inputDf

    if(!metadataColsSeq.isEmpty)
    for(metadataCols <- metadataColsSeq.get) {
      cols = cols ++ metadataCols
      metadataColsFullList = metadataColsFullList ++ metadataCols
    }

    var dfPoints =
      if (toGPS)
        inputDf.withColumn(MAGELLANPOINT_COL, CoordinatesUtils.magellanPointUDF(inputDf.col("lon"), inputDf.col("lat")))
      else
        inputDf.withColumn(MAGELLANPOINT_COL, CoordinatesUtils.magellanPointUDF(inputDf.col(xCol), inputDf.col(yCol)))

    if (!magellanIndex.isEmpty) {
      magellan.Utils.injectRules(inputDf.sparkSession)
      dfPoints = dfPoints.index(magellanIndex(0).get)
    }

    // Below join is for spark 2.3
    var dfPointsLabeled = broadcast(dfMultiPolygons).join(
      dfPoints,dfPoints.col(MAGELLANPOINT_COL) within dfMultiPolygons.col(polygonCol),"right_outer"
    )
      .select(cols.map(name => col(name)): _*)

    var dfPointsNoMatchLabelled =
      metadataColsFullList.foldLeft(dfPointsLabeled)
    {
      (df, metadataColName) =>
        df.withColumn(metadataColName, when(col(metadataColName).isNull, NOMATCH_IN_POLYGONS).otherwise(col(metadataColName)))
    }

    var resultDf: DataFrame = if(aggregateMetadata) {
      consolidateUnionMetadata(dfPointsNoMatchLabelled, groupByCols, metadataColsSeq,metadataColsFullList, METADATA_SEPARATOR, Some(DEFAULT_POLYGONS_METADATA))
    } else {
      dfPointsNoMatchLabelled
    }

    if (!outPartitions.isEmpty)
      resultDf = resultDf.repartition(outPartitions.get)

    if (outPath.isEmpty == false) {
      resultDf.write
        .mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .option("delimiter", "\t")
        .option("header", "false")
        .csv(outPath.get)
    }
   // resultDf.show(false)
    resultDf
  }


  def consolidateUnionMetadata(
                                df: DataFrame,
                                groupByCols: Seq[String],
                                metadataColsSeq : Option[Seq[Seq[String]]],
                                metadataColsFullList: Seq[String],
                                metadataSeparator: String,
                                defaultMetadata: Option[String]):
  DataFrame = {


    /* metadataWithCountCol will hold all metadata columns and one Count column for each Polygon file */
    var metadataWithCountCol = Seq[String]()
    for(metadataCols <- metadataColsSeq.get ){
      metadataWithCountCol = metadataWithCountCol++metadataCols ++ Seq(metadataCols(0)+"_Count")
    }

    /* udf to count number of metadata values by splitting the aggregated(concat_ws) string, also it filters NoMatch and IgnoreColumn values in the count  */
    //******* We can imporove here by simpling counting when the value is not in NOMATCH_IN_POLYGONS  and NOT_RELATED_COLUMN
    val countIgnoringNull = udf{(concatAggValues:String)=> (concatAggValues.split(metadataSeparator).toSeq filter (f => (!f.equals(NOMATCH_IN_POLYGONS) && !f.equals(NOT_RELATED_COLUMN)))).length}


    /* for all metadata columns, values are concatenated with a separator. For all Count columns, corresponding metadata column values are concatenated and split to get count value*/
    val aggColsAndCountCols: Seq[Column] = metadataWithCountCol.map(colName =>
      colName.contains("_Count") match {
        case false => concat_ws(metadataSeparator, collect_list(when(!(col(colName).isin(NOT_RELATED_COLUMN)), col(colName)))).alias(colName)
        case _ => countIgnoringNull(concat_ws(metadataSeparator, collect_list(when(!(col(colName.stripSuffix("_Count")).isin(NOT_RELATED_COLUMN)), col(colName.stripSuffix("_Count")))))).alias(colName)
      }
    )

    var aggregatedDf =  df
      .groupBy (groupByCols.map(name => col(name)):_*)
      .agg(aggColsAndCountCols.head, aggColsAndCountCols.tail:_*
      )
    aggregatedDf//aggregatedConsolidated
  }
}

