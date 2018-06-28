import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{udf, lit, col}
import org.rogach.scallop._

import magellan.Point

object JobGeoMapping {

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
              dfIn.withColumn(MAGELLANPOINT_COL, CoordinatesUtils.magellanPointUDF(dfIn.col("lat"), dfIn.col("lon")))
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

    // Scallop config parameters
    class CLOpts(arguments: Seq[String]) extends ScallopConf(arguments) {
      val separator       = opt[String](name="separator", required = false, default=Some("\t"))
      val magellanIndex   = opt[Int](name="magellan-index", required = false, default=Some(-1))
      val coordsInfo      = opt[String](name="coords-info", required = true)
      val toGPS           = opt[Boolean](name="to-gps", required = false, default=Some(false))
      val polygonsPath    = opt[String](name="polygons-path", required = false, default=Some(""))
      val inPartitions    = opt[Int](name="in-partitions", required = false, default=Some(-1))
      val outPartitions   = opt[Int](name="out-partitions", required = false, default=Some(-1))
      val metadataToFilter = opt[String](name="metadata-to-filter", required = false, default=Some(""))
      val others          = trailArg[List[String]](required = false)

      verify()
    }

    def main(args: Array[String]) {

        val clopts = new CLOpts(args)
        val separator         = clopts.separator()
        val inPartitions      = clopts.inPartitions()
        val outPartitions     = if (clopts.outPartitions() == -1) None else Some(clopts.outPartitions())
        val coordsInfo        = clopts.coordsInfo()
        val toGPS             = clopts.toGPS()
        val polygonsPath      = clopts.polygonsPath()
        val metadataToFilter  = if (clopts.metadataToFilter() == "") None else Some(clopts.metadataToFilter().split(",").toSeq)
        val others            = clopts.others()
        val outPath           = others(0)
        val magellanIndex = 
          if (clopts.magellanIndex() > 0 && clopts.magellanIndex() - (clopts.magellanIndex() % 5) > 5)
              Some(clopts.magellanIndex() - (clopts.magellanIndex() % 5))
          else
              None

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

        var dfPolygons  = CoordinatesUtils.loadPolygons(spark, polygonsPath, metadataToFilter, magellanIndex)
      
        runJob(dfIn2, xColName, yColName, toGPS = toGPS, dfPolygons = dfPolygons, metadataCols = metadataToFilter, magellanIndex = magellanIndex, outPartitions = outPartitions, outPath = Some(outPath))
    }
 }
