# Databricks notebook source

# MAGIC %md
# MAGIC # Produto Transacional — Utilitários de DataFrame
# MAGIC
# MAGIC Módulo responsável pelas transformações reutilizáveis aplicadas
# MAGIC aos DataFrames do produto transacional.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Converter valores decimais com segurança.
# MAGIC - Normalizar colunas de timestamp.
# MAGIC - Remove registros duplicados utilizando uma chave de negócio e critérios de ordenação previamente definidos.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------


def converter_decimal_seguro(
    nome_coluna: str,
    precisao: int = 18,
    escala: int = 2,
) -> Column:
    """
    Converte uma coluna para decimal com tratamento de formatos
    brasileiro e internacional.

    Valores que não puderem ser convertidos são retornados como nulos
    por meio de ``try_cast``.

    Args:
        nome_coluna: Nome da coluna que será convertida.
        precisao: Quantidade total de dígitos do decimal.
        escala: Quantidade de casas decimais.

    Returns:
        Expressão PySpark com o valor convertido para decimal.
    """
    coluna = f"`{nome_coluna}`"

    expressao = f"""
        try_cast(
            CASE
                WHEN trim(cast({coluna} AS string))
                    RLIKE '^-?[0-9]{{1,3}}(\\\\.[0-9]{{3}})+,[0-9]+$'
                THEN regexp_replace(
                    regexp_replace(
                        trim(cast({coluna} AS string)),
                        '\\\\.',
                        ''
                    ),
                    ',',
                    '.'
                )

                WHEN trim(cast({coluna} AS string))
                    RLIKE '^-?[0-9]+,[0-9]+$'
                THEN regexp_replace(
                    trim(cast({coluna} AS string)),
                    ',',
                    '.'
                )

                ELSE trim(cast({coluna} AS string))
            END
            AS DECIMAL({precisao},{escala})
        )
    """

    return F.expr(expressao)

# COMMAND ----------


def coluna_timestamp_segura(
    nome_coluna: str,
) -> Column:
    """
    Converte uma coluna para timestamp com valor de fallback.

    Quando a conversão não for possível, utiliza o timestamp atual.

    Args:
        nome_coluna: Nome da coluna que será convertida.

    Returns:
        Expressão PySpark do tipo timestamp.
    """
    return F.coalesce(
        F.to_timestamp(F.col(nome_coluna)),
        F.current_timestamp(),
    )

# COMMAND ----------


def deduplicar(
    dataframe: DataFrame,
    chaves: list[str],
    coluna_ordenacao: str,
) -> DataFrame:
    """
    Remove duplicidades utilizando chave de negócio e critérios
    determinísticos de ordenação.

    A função prioriza o registro mais recente com base na coluna
    principal de ordenação. Quando disponíveis, também utiliza
    ``data_ingestao`` e ``arquivo_origem`` como critérios adicionais.

    Args:
        dataframe: DataFrame que será deduplicado.
        chaves: Colunas que identificam unicamente o registro.
        coluna_ordenacao: Coluna principal usada para definir o registro
            mais recente.

    Returns:
        DataFrame contendo apenas um registro por chave de negócio.
    """
    criterios = [
        F.col(coluna_ordenacao).desc_nulls_last(),
    ]

    if "data_ingestao" in dataframe.columns:
        criterios.append(
            F.col("data_ingestao").desc_nulls_last()
        )

    if "arquivo_origem" in dataframe.columns:
        criterios.append(
            F.col("arquivo_origem").desc_nulls_last()
        )

    janela = (
        Window
        .partitionBy(*chaves)
        .orderBy(*criterios)
    )

    return (
        dataframe
        .dropDuplicates()
        .withColumn(
            "_numero_linha",
            F.row_number().over(janela),
        )
        .filter(F.col("_numero_linha") == 1)
        .drop("_numero_linha")
    )