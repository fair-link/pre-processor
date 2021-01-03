package com.fairlink.core.implementation

import com.fairlink.constants.Constants
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, current_date, date_add, sum}

case class ColumnNormalizer(spark: SparkSession) {
  def normalizeArtigos(disponibilidadeDaily: DataFrame, catalogo: DataFrame) = {
    disponibilidadeDaily
      .join(catalogo)
      .filter(col("artigo_disp").contains(col("artigo_catalogo")))
  }

  def prepareForDisponibilidade(produtosNoCatalogo: DataFrame)
  = {
    val expiration = Constants.createExpirationDf(spark)

    produtosNoCatalogo
      //add inspiration date
      .join(expiration, Seq("categoria_perecibilidade"), "inner")
      .withColumn("expiration_date", date_add(col("data_timestamp"), col("days_to_add")))
      .withColumn("disponibilidade_date", current_date())
      //filter columns
      .select("artigo_catalogo", "quantidade", "supermercado", "expiration_date", "disponibilidade_date")
      .withColumnRenamed("artigo_catalogo", "artigo_disp")
      .groupBy("artigo_disp", "supermercado", "expiration_date", "disponibilidade_date")
      .agg(sum("quantidade").as("quantidade"))
  }

  def validateNormalization(produtosNoCatalogo: DataFrame, produtosNaoNormalizados: DataFrame, odate: String) = {
    val normalizadosCount = produtosNoCatalogo.count()
    val naoNormalizadosCount = produtosNaoNormalizados.count()
    if (normalizadosCount > naoNormalizadosCount) {
      throw ColumnNormalizerException(s"Normalização produziu mais produtos do que os que existiam. Verificar catalogo e disponibilidade para odate ${}")
    }
  }
}
