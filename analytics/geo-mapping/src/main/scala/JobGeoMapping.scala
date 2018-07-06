import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{udf, lit, col, concat_ws, collect_list, expr, size, split}
import org.rogach.scallop._

import magellan.Point

object JobGeoMapping {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
    def getSparkSession(name:String, consoleEcho:Boolean = true) : SparkSession = {
        val spark = SparkSession
          .builder()
          .appName(name)
          .config("spark.io.compression.codec", "lz4")
          .config("spark.executor.cores", "3")
          .getOrCreate()

        // For implicit conversions
        import spark.implicits._

        if (consoleEcho)
            spark.conf.getAll.foreach{ case (key, value) => println(s"\t$key : $value") }

        spark
    }

    /**
      *
      *
      * @param
      * @return
      */
      def loadMultiPolygons(spark:SparkSession,
        multiPolygonsPath:Seq[String],
        multiPolygonsMetadataToExtract:Option[Seq[String]] = None,
        magellanIndex:Seq[Option[Int]] = Seq(None),
        defaultPolygonsPath: Option[String] = None
      )  : Seq[DataFrame] = {

        var polygonsDfs = Seq[DataFrame]()

        for((polygonsPath, metadataToExtract, index) <- (multiPolygonsPath, multiPolygonsMetadataToExtract.get, magellanIndex).zipped.toSeq) {

            val metadataToFilter = metadataToExtract.split(",")

            var polygonsDf = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToFilter), index)

            if(!defaultPolygonsPath.isEmpty) {
              val defaultPolygonsDf = CoordinatesUtils.loadPolygons(spark, defaultPolygonsPath.get.toString, Some(metadataToFilter), index)

              polygonsDf = polygonsDf.union(defaultPolygonsDf)
            }

            polygonsDfs = polygonsDfs ++ Seq(polygonsDf)
        }

        polygonsDfs
    }

    /**
      *
      *
      * @param
      * @return
      */
    def combineLabeledDataframes(labeledDfs: Seq[DataFrame], joinCols: Seq[String]) : DataFrame = {
        // Expected common points data, do we need to do any check before join like (rows count and cols existence)?
        var combinedDf = labeledDfs(0)
        for(i  <-  1 until labeledDfs.length) {
          combinedDf = combinedDf.join(labeledDfs(i), joinCols, "outer")
        }

        combinedDf
    }

    /**
      *
      *
      * @param
      * @return
      */
    def aggregateLabelsPerPoint(df: DataFrame, groupByCols: Seq[String], aggegateCols: Seq[String]):  DataFrame = {
        val aggCols = aggegateCols.map(colName => expr(s"""concat_ws(",",collect_list($colName))""").alias(colName))
        val aggregatedDf =  df
        .groupBy (groupByCols.head, groupByCols.tail: _*)
        .agg(aggCols.head, aggCols.tail: _*)

        aggregatedDf
    }

    /**
      *
      *
      * @param
      * @return
      */
    def countLabelsPerPoint(df: DataFrame, labelColName: String, labelsSeperator: String, countAlias: String) : DataFrame = {
        df.withColumn(countAlias, size(split(col(labelColName), labelsSeperator)))
    }
    
    def runJob(
      df:DataFrame, 
      xCol:String,
      yCol:String,
      toGPS:Boolean = true,
      dfPolygons:DataFrame, 
      polygonCol:String = "polygon",
      metadataCols:Option[Seq[String]] = None,
      magellanIndex:Option[Int] = None,
      outPartitions:Option[Int] = None,
      outPath:Option[String] = None
    ) : DataFrame = {

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

        var dfPointsLabeled = dfPoints
          .join(dfPolygons)
          .where(dfPoints.col(MAGELLANPOINT_COL) within dfPolygons.col(polygonCol))
          .select(cols.map(name => col(name)):_*)

        if (!outPartitions.isEmpty)
            dfPointsLabeled = dfPointsLabeled.repartition(outPartitions.get)

        if (outPath.isEmpty == false)
            dfPointsLabeled.write
              .mode(SaveMode.Overwrite)
              .format("com.databricks.spark.csv")
              .option("delimiter", "\t")
              .option("header", "false")
              .csv(outPath.get)

        dfPointsLabeled
    }

    /**
      * TBD
      *
      * @param
      * @return
      */
    def runMultiPolygonJob(
      dfIn:DataFrame,
      xColName:String,
      yColName:String,
      toGPS:Boolean = true,
      dfMultiPolygons: Seq[DataFrame],
      polygonCol:String = "polygon",
      multiPolygonsMetadataCols:Option[Seq[String]] = None,
      magellanIndex:Seq[Option[Int]] = Seq(None),
      aggregateLabels:Boolean = false,
      outPartitions:Option[Int] = None,
      outPath:Option[String] = None
    ) : DataFrame = {

        var joinCols = dfIn.columns.toSeq
        if (toGPS) {
            joinCols = joinCols ++ Seq("lat", "lon")
        }

        var labeledDfs = Seq[DataFrame]()

        for((dfPolygons, metadataCols, index) <- (dfMultiPolygons, multiPolygonsMetadataCols.get, magellanIndex).zipped.toSeq) {

            val metadataToFilter = metadataCols.split(",")

            val pointsLabeledDf = runJob(dfIn, xColName, yColName, toGPS = toGPS, dfPolygons = dfPolygons, metadataCols = Some(metadataToFilter), magellanIndex = index, outPartitions = None, outPath = None)

            val resultDf = if(aggregateLabels) {
              val tmpDf = aggregateLabelsPerPoint(pointsLabeledDf, joinCols, metadataToFilter)
              countLabelsPerPoint(tmpDf, metadataToFilter(0), ",", s"$metadataToFilter(0)" + "Count")
            } else {
              pointsLabeledDf
            }

            labeledDfs = labeledDfs ++ Seq(resultDf)
        }

        var combinedDf = combineLabeledDataframes(labeledDfs, joinCols)

        if (!outPartitions.isEmpty)
            combinedDf = combinedDf.repartition(outPartitions.get)

        if (outPath.isEmpty == false)
          combinedDf.write
            .mode(SaveMode.Overwrite)
            .format("com.databricks.spark.csv")
            .option("delimiter", "\t")
            .option("header", "false")
            .csv(outPath.get)

        combinedDf
    }


    // Scallop config parameters
    class CLOpts(arguments: Seq[String]) extends ScallopConf(arguments) {
      val separator           = opt[String](name="separator", required = false, default=Some("\t"))
      val magellanIndex       = opt[String](name="magellan-index", required = false, default=Some("-1"))
      val coordsInfo          = opt[String](name="coords-info", required = true)
      val toGPS               = opt[Boolean](name="to-gps", required = false, default=Some(false))
      val polygonsPath        = opt[String](name="polygons-path", required = false, default=Some(""))
      val inPartitions        = opt[Int](name="in-partitions", required = false, default=Some(-1))
      val outPartitions       = opt[Int](name="out-partitions", required = false, default=Some(-1))
      val metadataToFilter    = opt[String](name="metadata-to-filter", required = false, default=Some(""))
      val defaultPolygonsPath = opt[String](name="default-polygons-path", required = false, default=Some(""))
      val aggregateLabels     = opt[Boolean](name="aggregate-labels", required = false, default=Some(false))
      val others              = trailArg[List[String]](required = false)

      verify()
    }

    /**
      * TBD
      *
      * @param
      * @return
      */
    def validateMagellanIndex(index: String, separator: String) : Seq[Option[Int]] = {
        val magellanIndex = index.split(separator)

        val retIndex = magellanIndex.map { idx =>
            val magellanIndexInt = idx.toInt
            if (magellanIndexInt > 0 && (magellanIndexInt - magellanIndexInt % 5) >= 5)
              Some(magellanIndexInt - (magellanIndexInt % 5))
            else
              None
        }

      retIndex
    }

    def main(args: Array[String]) {

        val clopts = new CLOpts(args)
        val separator           = clopts.separator()
        val inPartitions        = clopts.inPartitions()
        val outPartitions       = if (clopts.outPartitions() == -1) None else Some(clopts.outPartitions())
        val coordsInfo          = clopts.coordsInfo()
        val toGPS               = clopts.toGPS()
        val polygonsPath        = if(clopts.polygonsPath() == "") None else Some(clopts.polygonsPath().split("::").toSeq)
        val metadataToFilter    = if (clopts.metadataToFilter() == "") None else Some(clopts.metadataToFilter().split("::").toSeq)
        val others              = clopts.others()
        val outPath             = others(0)
        val magellanIndex       = validateMagellanIndex(clopts.magellanIndex(), ",")
        val defaultPolygonsPath = if(clopts.defaultPolygonsPath() == "") None else Some(clopts.defaultPolygonsPath())
        val aggregateLabels     = clopts.aggregateLabels()

        val tmp = coordsInfo.split("::")
        val xColName   = "_c%s".format(tmp(0))
        val yColName   = "_c%s".format(tmp(1))
        val coordsPath  = tmp(2)

        val spark = getSparkSession("JobGeoMapping")

        val dfIn = spark
          .read
          .format("com.databricks.spark.csv")
          .option("delimiter", separator)
          .option("header", "false")
          .csv(coordsPath) 

        val dfIn2 = 
          if (inPartitions > 0)
              dfIn.repartition(inPartitions)
          else
              dfIn

        val dfPolygons = loadMultiPolygons(spark, polygonsPath.get, metadataToFilter, magellanIndex, defaultPolygonsPath)

        runMultiPolygonJob(dfIn2, xColName, yColName, toGPS, dfPolygons, "polygon", metadataToFilter, magellanIndex, aggregateLabels, outPartitions = outPartitions, outPath = Some(outPath))
    }
 }
