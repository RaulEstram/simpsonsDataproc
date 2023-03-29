package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.EnvironmentVariables.{CountChaptersTag, ListWithoutColumnsTag}
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import scala.collection.convert.ImplicitConversions.{`collection AsScalaIterable`, `iterable AsScalaIterable`, `list asScalaBuffer`}

package object utils {
  case class Params(countChapters: Int, listWithoutColumns: String)

  implicit class FunctionsConfig(config: Config) extends IOUtils {

    def readData(input: String): DataFrame = read(config.getConfig(input))


    def getParams: Params = Params(
      countChapters = config.getInt(CountChaptersTag),
      listWithoutColumns = config.getString(ListWithoutColumnsTag)
    )
  }
}
