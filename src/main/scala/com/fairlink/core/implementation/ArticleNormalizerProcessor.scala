package com.fairlink.core.implementation

import com.fairlink.constants.Constants
import com.fairlink.core.interface.Processor
import org.apache.spark.sql.DataFrame

case class ArticleNormalizerProcessor(private val normalizer: ColumnNormalizer, private val odate: String) extends Processor {
  override def process(dfs: Map[String, DataFrame]): DataFrame = {

    val disponibilidadeDaily = dfs.apply(Constants.DAILY_DISPONIBILIDADE_DF_KEY)
    val catalogo = dfs.apply(Constants.CATALOGO_DF_KEY)

    val produtosNoCatalogo = normalizer.normalizeArtigos(disponibilidadeDaily, catalogo)
    normalizer.validateNormalization(produtosNoCatalogo, disponibilidadeDaily, odate)

    normalizer.prepareForDisponibilidade(produtosNoCatalogo)
  }
}
