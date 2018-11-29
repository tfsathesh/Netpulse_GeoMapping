import org.apache.sis.referencing.{CRS, CommonCRS}
import org.apache.sis.geometry.DirectPosition2D
import org.apache.spark.sql.functions.{udf, col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import magellan.Point
import org.apache.spark.sql.types.{DoubleType}
import org.apache.spark.sql.magellan.dsl.expressions._


object CoordinatesUtils {

  val crsBNG = CRS.forCode("EPSG:27700") // British National Grid
  val crsWGS84 = CRS.forCode("EPSG:4326") // GPS
  val transformBNGtoWGS84 = CRS.findOperation(crsBNG, crsWGS84, null).getMathTransform

  def bng2wgs84(x: Double, y: Double): Seq[Double] = {
    transformBNGtoWGS84.transform(new DirectPosition2D(x, y), null).getCoordinate
  }

  val bng2wgs84UDF = udf { (x: Double, y: Double) => bng2wgs84(x, y) }

  val magellanPointUDF = udf { (x: Double, y: Double) => Point(x, y) } //coords.toString } //Point(coord(0),coord(1)) }

  /** This function converts coordinates from BNG to EGS84. Converted coordinates will be added as
    * new columns (lat and lon).
    *
    * @param df   Data frame with BNG coordinates.
    * @param xCol x-centroid columns name
    * @param yCol y-centroid columns name
    * @return Returns data frame with lat and lon columns.
    */
  def toGPS(df: DataFrame, xCol: String, yCol: String): DataFrame = {
    df.withColumn("_bng2wgs84Buf",
      CoordinatesUtils.bng2wgs84UDF(
        df.col(xCol).cast(DoubleType),
        df.col(yCol).cast(DoubleType)
      ))
      .withColumn("lat", col("_bng2wgs84Buf").getItem(0))
      .withColumn("lon", col("_bng2wgs84Buf").getItem(1))
      .drop("_bng2wgs84Buf")
  }

  /** This function loads polygons from input file. GeoJson and shape files are supported.
    *
    * @param spark             Spark Session.
    * @param path              Path to load polygons set.
    * @param metadataToExtract Metadata to extract from polygons.
    * @param index             Magellan index.
    * @return Return data frame with loaded polygons and corresponding metadata.
    */
  def loadPolygons(
                    spark: SparkSession,
                    path: String,
                    metadataToExtract: Option[Seq[String]] = None,
                    index: Option[Int] = None
                  ): DataFrame = {

    var dfPartial = spark.read.format("magellan")

    if (path.endsWith("json"))
      dfPartial = dfPartial.option("type", "geojson")

    if (!index.isEmpty)
      dfPartial
        .option("magellan.index", "true")
        .option("magellan.index.precision", index.get.toString)

    var df = dfPartial.load(path)

    val metadataCol: String = "metadata"

    if (!metadataToExtract.isEmpty) {
      val getMetadataValue  = udf((_: Map[String, String]).getOrElse((_: String), "-") match {    //converted this line to Implicit -- (hash: Map[String, String], key: String) => hash.getOrElse(key, "-")
        case value if value != null && !value.isEmpty => value
        case _ => "-"
      })
      metadataToExtract.get.foreach { key =>
        df = df.withColumn(key, getMetadataValue(col(metadataCol), lit(key)))
      }
      df = df.drop(col(metadataCol))
    }

    if (!index.isEmpty)
      df = df.withColumn("index", col("polygon") index index.get)
    df.distinct()
  }

  implicit class NullOccludingMap[K, V](private val underlying: Map[K, V]) extends AnyVal {
    def getNonNullOrElse(key: K, default: V): V = {
      underlying.get(key) match {
        case Some(value) if value != null => value
        case _ => default
      }
    }
  }

  /** This function loads one or more polygons sets. This function calls loadPolygons function
    * repeatedly to load polygons and returns sequence of loaded data frames.
    *
    * @param spark                Spark Session.
    * @param pathSeq              Path(s) to load polygons set.
    * @param metadataToExtractSeq Metadata to extract from polygons.
    * @param indexSeq             Magellan index(s).
    * @return Returns sequence of loaded data frames.
    */
  def loadMultiPolygons(spark: SparkSession,
                        pathSeq: Seq[String],
                        metadataToExtractSeq: Option[Seq[Seq[String]]] = None,
                        indexSeq: Seq[Option[Int]] = Seq(None)
                       ): Seq[DataFrame] = {

    var polygonsDfs = Seq[DataFrame]()

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

    //
    for ((polygonsPath, metadataToExtract, index) <- (pathSeq, metadataSeqIn, indexSeqIn).zipped.toSeq) {
      val df = CoordinatesUtils.loadPolygons(spark, polygonsPath, Some(metadataToExtract), index)
      polygonsDfs = polygonsDfs ++ Seq(df)
    }

    polygonsDfs

  }

  /** This function loads default polygons set. Based on the metadata information
    * same polygons set will be loaded but different metadata information.
    *
    * @param spark                Spark Session
    * @param defaultPolygonsPath  Default polygons set path
    * @param metadataToExtractSeq List of metadata to extract from polygons set
    * @param indexSeq             Magellan index
    * @return returns sequence of loaded data frames.
    */
  def loadDefaultPolygons(spark: SparkSession,
                          defaultPolygonsPath: String,
                          metadataToExtractSeq: Option[Seq[Seq[String]]] = None,
                          indexSeq: Seq[Option[Int]] = Seq(None)
                         ): Seq[DataFrame] = {

    var polygonsDfs = Seq[DataFrame]()

    for ((metadataToExtract, index) <- (metadataToExtractSeq.getOrElse(Seq(Seq(""))), indexSeq).zipped.toSeq) {

      val df = CoordinatesUtils.loadPolygons(spark, defaultPolygonsPath, Some(metadataToExtract), index)

      polygonsDfs = polygonsDfs ++ Seq(df)
    }

    polygonsDfs
  }

  /** This function combines two sequence of polygons set.
    *
    * @param  polygonsDfSeq        Normal polygons set(s)
    * @param  defaultPolygonsDfSeq Default polygons set(s)
    * @return Returns union of data frame.
    */
  def unionOfPolygonsDf(polygonsDfSeq: Seq[DataFrame], defaultPolygonsDfSeq: Seq[DataFrame]): Seq[DataFrame] = {
    var unionDfs = Seq[DataFrame]()

    for ((polygonsDf, defaultPolygonsDf) <- (polygonsDfSeq, defaultPolygonsDfSeq).zipped.toSeq) {
      val resDf = polygonsDf.union(defaultPolygonsDf)

      unionDfs = unionDfs ++ Seq(resDf)
    }

    unionDfs
  }
}
