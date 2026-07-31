# Databricks notebook source

# MAGIC %md
# MAGIC # Produto Transacional — SCD Tipo 2
# MAGIC
# MAGIC Módulo responsável pelo histórico de alterações da camada Prata.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Identificar registros novos ou alterados.
# MAGIC - Encerrar a versão ativa anterior.
# MAGIC - Inserir uma nova versão do registro.
# MAGIC - Manter apenas uma versão ativa por chave.

# COMMAND ----------

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from produto_transacional.utilitarios.tabelas import (
    alinhar_schema_com_tabela,
    tabela_existe,
)

# COMMAND ----------


def executar_scd_tipo_2(
    spark: SparkSession,
    dataframe: DataFrame,
    tabela_destino: str,
    chaves: list[str],
    colunas_comparacao: list[str],
    coluna_inicio_vigencia: str = "data_inicio_vigencia",
    coluna_fim_vigencia: str = "data_fim_vigencia",
    coluna_registro_ativo: str = "registro_ativo",
) -> None:
    """
    Executa a carga SCD Tipo 2 em uma tabela Delta.
    """
    instante_processamento = datetime.now(timezone.utc)

    colunas_scd = {
        coluna_inicio_vigencia,
        coluna_fim_vigencia,
        coluna_registro_ativo,
    }

    colunas_origem = [
        coluna
        for coluna in dataframe.columns
        if coluna not in colunas_scd
    ]

    dataframe_origem = dataframe.select(*colunas_origem)

    if not tabela_existe(spark, tabela_destino):
        (
            dataframe_origem
            .withColumn(
                coluna_inicio_vigencia,
                F.lit(instante_processamento).cast("timestamp"),
            )
            .withColumn(
                coluna_fim_vigencia,
                F.lit(None).cast("timestamp"),
            )
            .withColumn(
                coluna_registro_ativo,
                F.lit(True),
            )
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(tabela_destino)
        )

        return

    # COMMAND ----------

    dataframe_ativo = (
        spark.table(tabela_destino)
        .filter(F.col(coluna_registro_ativo) == F.lit(True))
        .alias("destino")
    )

    dataframe_origem = dataframe_origem.alias("origem")

    condicao_chaves = [
        F.col(f"origem.{chave}").eqNullSafe(
            F.col(f"destino.{chave}")
        )
        for chave in chaves
    ]

    condicao_join = condicao_chaves[0]

    for condicao in condicao_chaves[1:]:
        condicao_join = condicao_join & condicao

    condicao_alteracao = None

    for coluna in colunas_comparacao:
        coluna_alterada = ~F.col(
            f"origem.{coluna}"
        ).eqNullSafe(
            F.col(f"destino.{coluna}")
        )

        if condicao_alteracao is None:
            condicao_alteracao = coluna_alterada
        else:
            condicao_alteracao = (
                condicao_alteracao | coluna_alterada
            )

    # COMMAND ----------

    registros_avaliados = (
        dataframe_origem
        .join(
            dataframe_ativo,
            condicao_join,
            "left",
        )
        .withColumn(
            "_registro_existente",
            F.col(f"destino.{chaves[0]}").isNotNull(),
        )
        .withColumn(
            "_registro_alterado",
            F.coalesce(
                condicao_alteracao,
                F.lit(False),
            ),
        )
    )

    registros_alterados = (
        registros_avaliados
        .filter(
            F.col("_registro_existente")
            & F.col("_registro_alterado")
        )
        .select(
            *[
                F.col(f"origem.{chave}").alias(chave)
                for chave in chaves
            ]
        )
        .dropDuplicates()
    )

    novos_ou_alterados = (
        registros_avaliados
        .filter(
            (~F.col("_registro_existente"))
            | F.col("_registro_alterado")
        )
        .select(
            *[
                F.col(f"origem.{coluna}").alias(coluna)
                for coluna in colunas_origem
            ]
        )
    )

    # COMMAND ----------

    if not registros_alterados.isEmpty():
        condicao_merge = " AND ".join(
            [
                f"destino.{chave} = origem.{chave}"
                for chave in chaves
            ]
            + [f"destino.{coluna_registro_ativo} = true"]
        )

        (
            DeltaTable.forName(
                spark,
                tabela_destino,
            )
            .alias("destino")
            .merge(
                registros_alterados.alias("origem"),
                condicao_merge,
            )
            .whenMatchedUpdate(
                set={
                    coluna_fim_vigencia: (
                        f"timestamp'{instante_processamento.isoformat()}'"
                    ),
                    coluna_registro_ativo: "false",
                }
            )
            .execute()
        )

    # COMMAND ----------

    if novos_ou_alterados.isEmpty():
        return

    novas_versoes = (
        novos_ou_alterados
        .withColumn(
            coluna_inicio_vigencia,
            F.lit(instante_processamento).cast("timestamp"),
        )
        .withColumn(
            coluna_fim_vigencia,
            F.lit(None).cast("timestamp"),
        )
        .withColumn(
            coluna_registro_ativo,
            F.lit(True),
        )
    )

    novas_versoes = alinhar_schema_com_tabela(
        spark,
        novas_versoes,
        tabela_destino,
    )

    (
        novas_versoes.write
        .format("delta")
        .mode("append")
        .saveAsTable(tabela_destino)
    )