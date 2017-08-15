package io.mmgs.ex01

import com.google.gson.GsonBuilder

/**
  * Created by mmgs on 8/14/17.
  */

case class Value(value: String, amount: Long)

case class ColumnReport(name: String, uniqueValues: Long, values: Array[Value]) {
  def toJson: String = {
    val gson = new GsonBuilder().setPrettyPrinting().create
    gson.toJson(this)
  }
}

