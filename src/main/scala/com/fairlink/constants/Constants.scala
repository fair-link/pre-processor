package com.fairlink.constants

import org.apache.spark.sql.{DataFrame, SparkSession}

object Constants {
  val DATE_FORMAT: String = "yyyy-MM-dd"


  //Table Names
  val TABLE_NAME_DISPONIBILIDADE_RAW: String = "disponibilidade_raw"
  val TABLE_NAME_CATALOGO: String = "catalogo"
  val TABLE_NAME_ARTIGOS_DISPONIVEIS: String = "disponibilidade"

  // DF Identifiers
  val CATALOGO_DF_KEY: String = "catalogo"
  val DAILY_DISPONIBILIDADE_DF_KEY = "disponibilidadeDaily"

  //Db_configurations
  val DB_HOST: String = "0.0.0.0"
  val DB_NAME: String = "postgres"
  val DB_USER: String = "admin"
  val DB_PASS: String = "secret"

  def createExpirationDf(spark: SparkSession): DataFrame = {
    val columns = Seq("categoria_perecibilidade", "days_to_add")
    val data = Seq(("A", 1), ("B", 4), ("C", 10), ("D", 20))
    val rdd = spark.sparkContext.parallelize(data)
    spark.createDataFrame(rdd).toDF(columns: _*)
  }
}
