package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.fields._
import org.apache.spark.sql.functions.{col, count, sum}
import org.apache.spark.sql.{Column, DataFrame, functions}


package object transforms {

  implicit class DataFrameTransforms(dataframe: DataFrame) {

    /**
     * Función que Limpia la data de los simpsons por `rating`, `votes` y `viewers_in_millions`
     *
     * @return `DataFrame` Limpio
     */
    def cleanSimpsonsData: DataFrame = dataframe.cleanRating.cleanVotes.cleanViewersInMillions

    /**
     * Método que limpio el data de los simpsons por `rating`
     *
     * @return `DataFrame` Limpio por `rating`
     */
    def cleanRating: DataFrame = dataframe.filter(Rating.column.isNotNull)

    /**
     * Método que limpio el data de los simpsons por `votes`
     *
     * @return `DataFrame` Limpio por `votes`
     */
    def cleanVotes: DataFrame = dataframe.filter(Votes.column.isNotNull)

    /**
     * Método que limpio el data de los simpsons por `viewers_in_millions`
     *
     * @return `DataFrame` Limpio por `viewers_in_millions`
     */
    def cleanViewersInMillions: DataFrame = dataframe.filter(ViewersInMillions.column.isNotNull)

    /**
     * Método que regresa un `DataFrame` con la temporada de los simpsons con el `rating` mas alto
     *
     * @return `DataFrame` de la temporada con mas `rating`
     */
    def getBestTemp: DataFrame = {
      val colName: String = "total_rating"
      // creamos un df agrupado por ratings y calculamos sus respectivos ratings
      val season = dataframe.groupBy(Season.column).agg(functions.sum(Rating.column) alias colName)
      // obtenemos el registro con el rating mas alto
      val maxRow = season.reduce(
        (row1, row2) => if (row1.getAs[Double](colName) > row2.getAs[Double](colName)) row1 else row2
      )
      // regresamos el df con el rating mas alto
      season.filter(col(colName) === maxRow.getAs[Double](colName))
    }

    /**
     * Método que regresa un `DataFrame` con la año de los simpsons con la mayor cantidad de `viewers_in_millions` por
     * `original_air_year`
     *
     * @return `DataFrame` de la temporada con mas `viewers_in_millions`
     */
    def getBestYearByViewers: DataFrame = {
      val colName: String = "viewers_in_millions_year"
      // creamos un df agrupado por Original air year y obtenemos la suma de sus viewers
      val df = dataframe.groupBy(OriginalAirYear.column).agg(sum(ViewersInMillions.column) alias colName)
      // obtenemos el registro con el rating mas alto
      val maxRow = df.reduce(
        (row1, row2) => if (row1.getAs[Double](colName) > row2.getAs[Double](colName)) row1 else row2
      )
      // regresamos el df con el año con mas viewers
      df.filter(col(colName) === maxRow.getAs[Double](colName))
    }

    /**
     * Método que regresa un `DataFrame` con el mejor capitulo según el `score` que se calcula con
     * `rating` * `viewers_in_millions`
     *
     * @return `DataFrame` con el capitulo con el mejor `score`
     */
    def getBestChapter: DataFrame = {
      // agregamos el score y seleccionamos las columnas que nos importan
      val df: DataFrame = dataframe.addColumn(Score()).select(Title.column, Score.column)
      // obtenemos el row con el mejor score
      val maxRow = df.reduce(
        (row1, row2) => if (row1.getAs[Float](Score.name) > row2.getAs[Float](Score.name)) row1 else row2
      )
      // regresamos un dataframe con el mejor capitulo
      df.filter(Score.column === maxRow.getAs[Float](Score.name))
    }

    /**
     * Método que agrega una columna al dataframe
     *
     * @param column `Column`
     * @return `DataFrame` Original con la nueva columna
     */
    def addColumn(column: Column): DataFrame = {
      val columns: Array[Column] = dataframe.columns.map(col) :+ column
      dataframe.select(columns: _*)
    }

    /**
     * Método que regresa un `DataFrame` con los mejores capítulos por `season` de los simpsons
     * la cantidad de capítulos que se mostraran por `season` esta determinado por `countChapters`
     *
     * @param countChapters `Int`
     * @return `DataFrame`
     */
    def bestChaptersBySeason(countChapters: Int = 3): DataFrame = {
      // creamos un dataFrame para saber cuantos capítulos hay por temporada y obtenemos únicamente
      // las temporadas que tengan la cantidad minima de capítulos deseados
      val cantidadCapitulos: DataFrame = dataframe.groupBy(Season.column)
        .agg(count(Season.column) alias "count")
        .filter(col("count") >= countChapters)

      // creamos otro dataframe que va a tener toda la información de únicamente las temporadas
      // que estén en el anterior dataframe y le agregamos su score
      val data: DataFrame = dataframe.join(cantidadCapitulos, Season.name)
        .addColumn(Score())

      // le agregamos una columna Top que nos ayudara a definir el top de los capítulos por temporada
      // posteriormente regresamos solamente los capítulos que estén en el top dependiendo del countChapters
      data.addColumn(Top())
        .filter(Top.column <= countChapters)
        .orderBy(Season.column, Top.column)
    }

    /**
     * Método que sirve para regresar un dataframe sin las columnas pasadas en el parámetro en forma de un
     * `Array[String]`, por ultimo si tiene el campo `original_air_date` lo pasa a un tipo de dato `Date`
     * para su posterior escritura en formato parquet
     *
     * @param withoutColumns Array[String]
     * @return `DataFrame`
     */
    def selectColumnsToWrite(withoutColumns: Array[String]): DataFrame = {
      // Obtenemos un Array con todos las columnas del dataframe, menos las que se pasan por el parámetro
      val columns: Array[Column] = dataframe.columns
        .filter(!withoutColumns.contains(_))
        .map(col)
      // generamos el dataframe
      val df = dataframe.select(columns: _*)
      // si el dataframe contiene la columna "original_air_date" realizamos un casting a Date y regresamos el dataframe
      val fecha = "original_air_date"
      if (df.columns.contains(fecha)) {
        df.select(df.columns.filter(_ != fecha).map(col) :+ col(fecha).cast("Date").alias(fecha): _*)
      } else {
        df
      }

    }
  }

}
