package com.bbva.datioamproduct.fdevdatio.common

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.row_number

package object fields {
  case object Rating extends Field {
    override val name: String = "rating"
  }

  case object Votes extends Field {
    override val name: String = "votes"
  }

  case object ViewersInMillions extends Field {
    override val name: String = "viewers_in_millions"
  }

  case object Season extends Field {
    override val name: String = "season"
  }

  case object OriginalAirYear extends Field {
    override val name: String = "original_air_year"
  }

  case object Title extends Field {
    override val name: String = "title"
  }

  case object Score extends Field {
    override val name: String = "score"

    def apply(): Column = (Rating.column * ViewersInMillions.column) alias name

  }

  case object Top extends Field {
    override val name: String = "top"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(Season.column).orderBy(Score.column.desc)

      row_number() over w alias "top"
    }
  }
}
