
# MAGIC %md
# MAGIC # Produto Transacional — Qualidade
# MAGIC
# MAGIC Módulo responsável pelas validações genéricas utilizadas na
# MAGIC camada Prata.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Adicionar motivos de quarentena.
# MAGIC - Separar registros válidos e inválidos.
# MAGIC - Centralizar regras reutilizadas pelos processamentos.

# COMMAND ----------

from collections.abc import Sequence
from typing import TypeAlias

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


RegraQualidade: TypeAlias = tuple[Column, str]


# COMMAND ----------

def adicionar_motivo_quarentena(
    dataframe: DataFrame,
    regras: Sequence[RegraQualidade],
) -> DataFrame:
    """
    Adiciona a coluna motivo_quarentena ao DataFrame.
    """

    motivos = [
        F.when(condicao, F.lit(mensagem))
        for condicao, mensagem in regras
    ]

    return dataframe.withColumn(
        "motivo_quarentena",
        F.concat_ws("; ", *motivos),
    )


# COMMAND ----------

def separar_validos_invalidos(
    dataframe: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """
    Separa registros válidos e inválidos.
    """

    motivo_quarentena = F.trim(
        F.coalesce(
            F.col("motivo_quarentena"),
            F.lit(""),
        )
    )

    invalidos = dataframe.filter(
        F.length(motivo_quarentena) > 0
    )

    validos = (
        dataframe
        .filter(F.length(motivo_quarentena) == 0)
        .drop("motivo_quarentena")
    )

    return validos, invalidos