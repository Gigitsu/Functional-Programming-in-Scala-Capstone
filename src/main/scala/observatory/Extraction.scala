package observatory

import java.time.LocalDate

import observatory.utils.BaseSqlSpark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * 1st milestone: data extraction
  */
object Extraction extends BaseSqlSpark {

  case class StationWithTemperatures(id: String, latitude: Double, longitude: Double, year: Year, month: Int, day: Int, temperature: Temperature)

  case class TemperatureRecord(id: String, year: Year, month: Int, day: Int, temperature: Temperature)

  case class Station(id: String, latitude: Double, longitude: Double)

  case class Date(year: Year, month: Int, day: Int)

  def toLocalDate(d: Date): LocalDate = LocalDate.of(d.year, d.month, d.day)

  import spark.implicits._

  private lazy val idCol = concat_ws("%", coalesce('_c0, lit("")), '_c1).alias("id")

  private def loadStation(file: String) = spark.read
    .csv(resourcePath(file))
    .select(
      idCol,
      '_c2.alias("latitude").cast(DoubleType),
      '_c3.alias("longitude").cast(DoubleType)
    )
    .where('latitude.isNotNull && 'longitude.isNotNull && 'latitude =!= 0 && 'longitude =!= 0)
    .as[Station]

  private def loadTemperatures(file: String, year: Year) = spark.read
    .csv(resourcePath(file))
    .select(
      idCol,
      lit(year).alias("year"),
      '_c2.alias("month").cast(IntegerType),
      '_c3.alias("day").cast(IntegerType),
      '_c4.alias("temperature").cast(DoubleType)
    )
    .as[TemperatureRecord]

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    loadTemperatures(temperaturesFile, year)
      .join(loadStation(stationsFile), "id" :: Nil)
      .as[StationWithTemperatures]
      .map(st => (Date(st.year, st.month, st.day), Location(st.latitude, st.longitude), (st.temperature - 32) / 1.8))
      .collect
      .map(x => (toLocalDate(x._1), x._2, x._3))
      .seq
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    records
      .par
      .groupBy(_._2)
      .mapValues(x => x.map(_._3).sum / x.size)
      .seq
  }

}
