package com.bbva.datioamproduct.fdevdatio.common.fields

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

/**
 * Trait que se utiliza simplemente para crear case object que hereden de este trait
 * Para un manejo mas fácil y elegante de las columnas de los Dataframes
 */
trait Field {
  val name: String
  lazy val column: Column = col(name)

}
