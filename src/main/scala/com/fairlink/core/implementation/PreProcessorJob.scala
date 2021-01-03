package com.fairlink.core.implementation

import com.fairlink.constants.Constants
import com.fairlink.core.interface.{Job, Processor}
import com.fairlink.infrastructure.implementation.{PgTableReader, PgTableWriter}
import com.fairlink.infrastructure.interface.{TableReader, TableWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, substring}

case class PreProcessorJob(private val odate: String) extends Job {
  private val spark: SparkSession = SparkSession
    .builder
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  override val reader: TableReader = PgTableReader(spark, Constants.DB_HOST, Constants.DB_NAME, Constants.DB_USER, Constants.DB_PASS)
  override val processor: Processor = ArticleNormalizerProcessor(ColumnNormalizer(spark), odate)
  override val writer: TableWriter = PgTableWriter(spark, Constants.DB_HOST, Constants.DB_NAME, Constants.DB_USER, Constants.DB_PASS)

  override def run(): Unit = {
    import spark.implicits._

    val disponibilidadeDaily = reader.read(Constants.TABLE_NAME_DISPONIBILIDADE_RAW)
      .where(substring($"data_timestamp", 1, 10) === lit(odate))

    val catalogo = reader.read(Constants.TABLE_NAME_CATALOGO)

    val dfMap = Map(Constants.DAILY_DISPONIBILIDADE_DF_KEY -> disponibilidadeDaily, Constants.CATALOGO_DF_KEY -> catalogo)

    val produtosDisponíveis = processor.process(dfMap)

    writer.write(produtosDisponíveis, Constants.TABLE_NAME_ARTIGOS_DISPONIVEIS)
  }


}
