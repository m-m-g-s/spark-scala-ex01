package io.mmgs.ex01

import java.io.FileReader
import java.util
import java.util.{ArrayList, LinkedHashMap}

import org.yaml.snakeyaml.Yaml


case class Column(number: Int, name: String, dataType: String) {
  validate(number)
  validate(name)
  validate(dataType)

  private def validate[T](data: T): Unit = {
    if (data == null) throw new NoSuchElementException("Column number property is missing in data model definition")
  }
}

class Schema(val name: String, val columns: List[Column]) {
  def canEqual(a: Any): Boolean = a.isInstanceOf[Schema]

  override def equals(that: scala.Any): Boolean = {
    that match {
      case that: Schema => that.canEqual(this) && this.name == that.name && this.columns == that.columns
      case _ => false
    }
  }
}

object Schema {
  def fromFile(path: String): Schema = {
    val yaml = (new Yaml).load(new FileReader(path))
    val document = yaml.asInstanceOf[util.LinkedHashMap[String, AnyVal]]
    val schema = document.get("schema").asInstanceOf[util.ArrayList[util.LinkedHashMap[String, AnyVal]]] match {
      case x if x == null || x.isEmpty => throw new NoSuchElementException("Schema property is missing in data model definition")
      case x => x
    }
    val columns = for (i <- 0 until schema.size();
                       number = schema.get(i).get("number").asInstanceOf[Int];
                       name = schema.get(i).get("column").toString;
                       dataType = schema.get(i).get("dataType").toString;
                       column = Column(number, name, dataType)
    ) yield column

    val table = document.get("table").asInstanceOf[String] match {
      case x if x == null || x.isEmpty => throw new NoSuchElementException("Table property is missing in data model definition")
      case x => x
    }
    new Schema(table, columns.toList)
  }

  def columnsList(table: Schema): List[String] = {
    val columns = for (column <- table.columns) yield column.name
    columns
  }

}
