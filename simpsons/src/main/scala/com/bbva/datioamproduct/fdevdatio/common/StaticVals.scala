package com.bbva.datioamproduct.fdevdatio.common

case object StaticVals {

  /**
   * Case object que contiene las variables estáticas para la configuración
   */
  case object ConfigConstants {
    val RootConfig: String = "simpsonsJob"
    val InputTag: String = s"$RootConfig.input"
    val OutputTag: String = s"$RootConfig.output"

    val InputSimpsonsTag: String = s"$InputTag.simpsons"
    val OutputSimpsonsTag: String = s"$OutputTag.simpsons"
  }

  /**
   * Case object que contiene las variables estáticas para las variables de entorno
   */
  case object EnvironmentVariables {
    private val RootParams: String = s"${ConfigConstants.RootConfig}.params"
    val CountChaptersTag: String = s"$RootParams.countChapters"
    val ListWithoutColumnsTag: String = s"$RootParams.lisColumns"
  }

  /**
   * Case object que contiene las variables estáticas para los joins de los Dataframes
   */
  case object Joins {
    val LeftAntiJoin: String = "left_anti"
    val LeftJoin: String = "left"
    val InnerJoin: String = "inner"
  }

  /**
   * Case object que contiene las variables estáticas para valores generales
   */
  case object Values {
    val SuccessNumber: Int = 0
    val ErrorNumber: Int = -1
    val Comma: String = ","
  }

}