import org.apache.sis.referencing.{CRS, CommonCRS}
import org.apache.sis.geometry.DirectPosition2D
import org.apache.spark.sql.functions.{udf, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import magellan.Point
import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.sql.magellan.dsl.expressions._

 
object CoordinatesUtils {
    
    val crsBNG      = CRS.forCode("EPSG:27700")  // British National Grid
    val crsWGS84    = CRS.forCode("EPSG:4326")   // GPS
    val transformBNGtoWGS84 = CRS.findOperation(crsBNG, crsWGS84, null).getMathTransform

    def bng2wgs84(x:Double, y:Double) : Seq[Double] = {
        transformBNGtoWGS84.transform(new DirectPosition2D(x,y), null).getCoordinate
    }

    val bng2wgs84UDF        = udf{ (x:Double,y:Double) => bng2wgs84(x, y) }

    val magellanPointUDF    = udf{ (x:Double,y:Double) => Point(x,y) } //coords.toString } //Point(coord(0),coord(1)) }

    def toGPS(df:DataFrame, xCol:String, yCol:String) : DataFrame = {
          df.withColumn("_bng2wgs84Buf",
              CoordinatesUtils.bng2wgs84UDF(
                df.col(xCol).cast(DoubleType),
                df.col(yCol).cast(DoubleType)
          ))
          .withColumn("lat", col("_bng2wgs84Buf").getItem(0))
          .withColumn("lon", col("_bng2wgs84Buf").getItem(1))
          .drop("_bng2wgs84Buf")
    }

    def loadPolygons(
      spark:SparkSession, 
      path:String, 
      metadataToExtract:Option[Seq[String]] = None,
      index:Option[Int] = None
    ) : DataFrame = {
        
        var dfPartial = spark.read.format("magellan")

        if (path.endsWith("json"))
            dfPartial = dfPartial.option("type", "geojson")

        if (!index.isEmpty)
              dfPartial
                .option("magellan.index", "true")
                .option("magellan.index.precision", index.get.toString)

        var df = dfPartial.load(path)

        val metadataCol = "metadata"

        if (!metadataToExtract.isEmpty) {
            val getMetadataValue = udf( (hash:Map[String,String], key:String) => hash.getOrElse(key, "-"))
            metadataToExtract.get.foreach{ key =>
               df = df.withColumn(key, getMetadataValue(col(metadataCol), lit(key))) 
            }
          df = df.drop(col(metadataCol))
        }

        if (!index.isEmpty)
            df = df.withColumn("index", col("polygon") index index.get)
        df
    }

    /**
      *
      *
      * @param
      * @return
      */
    def loadMultiPolygons(spark:SparkSession,
                          pathSeq:Seq[String],
                          metadataToExtractSeq:Option[Seq[Seq[String]]] = None,
                          indexSeq:Seq[Option[Int]] = Seq(None)
                         )  : Seq[DataFrame] = {

      var polygonsDfs = Seq[DataFrame]()

      for((polygonsPath, metadataToExtract, index) <- (pathSeq, metadataToExtractSeq.get, indexSeq).zipped.toSeq) {

        val df = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToExtract), index)

        polygonsDfs = polygonsDfs ++ Seq(df)
      }

      polygonsDfs
    }

    /**
      *
      *
      * @param
      * @return
      */
    def loadDefaultPolygons(spark:SparkSession,
                           defaultPolygonsPath:String,
                           metadataToExtractSeq:Option[Seq[Seq[String]]] = None,
                           indexSeq:Seq[Option[Int]] = Seq(None)
                         )  : Seq[DataFrame] = {

      var polygonsDfs = Seq[DataFrame]()

      for((metadataToExtract, index) <- (metadataToExtractSeq.get, indexSeq).zipped.toSeq) {

        val df = CoordinatesUtils.loadPolygons(spark, defaultPolygonsPath, Some(metadataToExtract), index)

        polygonsDfs = polygonsDfs ++ Seq(df)
      }

      polygonsDfs
    }

    /**
      *
      *
      * @param
      * @return
      */
    def unionOfPolygonsDf(polygonsDfSeq : Seq[DataFrame], defaultPolygonsDfSeq: Seq[DataFrame]): Seq[DataFrame] = {
      var unionDfs = Seq[DataFrame]()

      for((polygonsDf, defaultPolygonsDf) <- (polygonsDfSeq, defaultPolygonsDfSeq).zipped.toSeq) {
        val resDf = polygonsDf.union(defaultPolygonsDf)

        unionDfs = unionDfs ++ Seq(resDf)
      }

      unionDfs
    }
}
