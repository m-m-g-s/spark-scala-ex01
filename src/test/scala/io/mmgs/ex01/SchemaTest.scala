package io.mmgs.ex01


import java.io.FileNotFoundException

import org.scalatest.{FeatureSpec, GivenWhenThen}

/**
  * Created by mmgs on 8/14/17.
  */
class SchemaTest extends FeatureSpec with GivenWhenThen {
  def getResourcePath(file: String): String = getClass.getClassLoader.getResource(file).getPath

  feature("Parsing yaml data model configuration") {
    scenario("Correct data model") {
      val targetDirPath = getResourcePath("data-model.yaml")

      val expected = new Schema("test_table",
        List(
          Column(1, "col1", "string"),
          Column(2, "col2", "int"),
          Column(3, "col3", "boolean"),
          Column(4, "col4", "date")
        ))

      val got = Schema.fromFile(targetDirPath)

      assert(expected === got)
    }

    scenario("Missing table property") {
      val targetDirPath = getResourcePath("data-model-no-table-prop.yaml")

      intercept[NoSuchElementException] {
        Schema.fromFile(targetDirPath)
      }
    }

    scenario("Missing table property value") {
      val targetDirPath = getResourcePath("data-model-no-table-val.yaml")

      intercept[NoSuchElementException] {
        Schema.fromFile(targetDirPath)
      }
    }

    scenario("No data model file provided") {
      intercept[FileNotFoundException] {
        Schema.fromFile("/no/such/file.yaml")
      }
    }

    scenario("Missing schema property") {
      val targetDirPath = getResourcePath("data-model-no-schema-prop.yaml")

      intercept[NoSuchElementException] {
        Schema.fromFile(targetDirPath)
      }
    }

    scenario("Missing schema property value") {
      val targetDirPath = getResourcePath("data-model-no-schema-val.yaml")

      intercept[NoSuchElementException] {
        Schema.fromFile(targetDirPath)
      }
    }

    // TODO: check missing column properties


  }
}