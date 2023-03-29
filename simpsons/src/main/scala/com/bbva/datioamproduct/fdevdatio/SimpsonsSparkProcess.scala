package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVals.ConfigConstants.{InputSimpsonsTag, OutputSimpsonsTag}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.Values.{Comma, ErrorNumber, SuccessNumber}
import com.bbva.datioamproduct.fdevdatio.transforms.DataFrameTransforms
import com.bbva.datioamproduct.fdevdatio.utils.{FunctionsConfig, IOUtils, Params}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class SimpsonsSparkProcess extends SparkProcess with IOUtils {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Código que realizara el procesamiento para el problema de los Simpsons
   *
   * @param runtimeContext RuntimeContext
   */
  override def runProcess(runtimeContext: RuntimeContext): Int = {

    Try {
      val config: Config = runtimeContext.getConfig
      val params: Params = config.getParams

      /**
       * Lectura del archivo CSV que contiene la data de los simpsons
       * Y Limpieza del la Data
       */
      // TODO: Agregar prueba unitaria y errores para fallo de lectura del archivo
      val simpsonsDf: DataFrame = config.readData(InputSimpsonsTag).cleanSimpsonsData

      /**
       * Obtener la mejor temporada de los simpsons de acuerdo al mayor ranting
       */
      val mejorTemporadaDf: DataFrame = simpsonsDf.getBestTemp
      mejorTemporadaDf.show()

      /**
       * Obtener el mejor año de los simpsons de acuerdo al número de vistas de los capítulos de dicho año
       */
      val mejorAnio: DataFrame = simpsonsDf.getBestYearByViewers
      mejorAnio.show()

      /**
       * Obtener el mejor capitulo de los simpsons de acuerdo al score
       */
      val mejorCapitulo: DataFrame = simpsonsDf.getBestChapter
      mejorCapitulo.show()

      /**
       * Obtener el top 3 de capítulos por cada temporada de acuerdo a su score
       */
      val mejoresCapitulos: DataFrame = simpsonsDf.bestChaptersBySeason(params.countChapters)
      mejoresCapitulos.show()

      /**
       * Preparación del dataframe para la escritura
       */
      val lista = params.listWithoutColumns.split(Comma).map(_.trim)
      val dataFrameWrite: DataFrame = mejoresCapitulos.selectColumnsToWrite(lista)


      write(dataFrameWrite, config.getConfig(OutputSimpsonsTag))

    } match {
      case Success(_) => SuccessNumber
      case Failure(exception: Exception) =>
        logger.error("Se produjo un error inesperado")
        exception.printStackTrace()
        ErrorNumber
    }
  }

  override def getProcessId: String = "SimpsonsSparkProcess"

}

