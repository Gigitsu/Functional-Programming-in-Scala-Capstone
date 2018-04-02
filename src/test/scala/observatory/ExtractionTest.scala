package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {
  test("shoud load data in csv format") {
    val a = Extraction.locateTemperatures(1975, "/stations.csv", "/1975.csv")
    print("gg")
  }
}