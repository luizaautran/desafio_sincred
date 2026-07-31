# Databricks notebook source

# MAGIC %md
# MAGIC # Produto Transacional — Quarentena
# MAGIC
# MAGIC Módulo responsável pela persistência dos registros que não
# MAGIC atenderam às regras de qualidade da camada Prata.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Criar a tabela de quarentena quando ela ainda não existir.
# MAGIC - Alinhar o schema dos registros inválidos à tabela de destino.
# MAGIC - Gravar os registros inválidos em uma tabela Delta.


# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession

from produto_transacional.utilitarios.tabelas import (
    alinhar_schema_com_tabela,
    tabela_existe,
)

# COMMAND ----------


def garantir_tabela_quarentena(
    spark: SparkSession,
    dataframe: DataFrame,
    tabela_destino: str,
) -> None:
    """
    Cria a tabela de quarentena caso ela não exista.
    """
    if tabela_existe(spark, tabela_destino):
        return

    (
        dataframe
        .limit(0)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(tabela_destino)
    )

# COMMAND ----------


def gravar_quarentena(
    spark: SparkSession,
    dataframe: DataFrame,
    tabela_destino: str,
) -> None:
    """
    Grava registros inválidos na tabela de quarentena.
    """
    garantir_tabela_quarentena(
        spark,
        dataframe,
        tabela_destino,
    )

    dataframe = alinhar_schema_com_tabela(
        spark,
        dataframe,
        tabela_destino,
    )

    (
        dataframe.write
        .format("delta")
        .mode("append")
        .saveAsTable(tabela_destino)
    )