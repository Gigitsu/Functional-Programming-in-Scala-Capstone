package observatory.utils

import java.nio.file.Paths

import org.apache.spark.sql.SparkSession

trait BaseSqlSpark {
  protected implicit val spark: SparkSession = SparkSession
    .builder()
    .appName(getClass.getSimpleName)
    .master("local")
    .getOrCreate()

  protected def resourcePath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

}
