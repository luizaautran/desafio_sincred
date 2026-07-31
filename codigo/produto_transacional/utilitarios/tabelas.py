
# MAGIC %md
# MAGIC # Produto Transacional — Utilitários de Tabelas
# MAGIC
# MAGIC Módulo responsável pelas operações reutilizáveis em tabelas
# MAGIC da camada transacional.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Verificar a existência de tabelas no catálogo.
# MAGIC - Alinhar o schema de DataFrames ao schema da tabela de destino.
# MAGIC - Sobrescrever tabelas Delta com atualização de schema.

# COMMAND ----------

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# COMMAND ----------


def tabela_existe(
    spark: SparkSession,
    nome_tabela: str,
) -> bool:
    """
    Verifica se uma tabela existe no catálogo do Spark.

    Args:
        spark: Sessão ativa do Spark.
        nome_tabela: Nome completo da tabela.

    Returns:
        True quando a tabela existe; caso contrário, False.
    """
    return spark.catalog.tableExists(nome_tabela)

# COMMAND ----------


def alinhar_schema_com_tabela(
    spark: SparkSession,
    dataframe: DataFrame,
    tabela_destino: str,
) -> DataFrame:
    """
    Alinha o schema do DataFrame ao schema da tabela de destino.

    As colunas existentes são convertidas para o tipo esperado.
    Colunas ausentes são adicionadas com valor nulo.

    Args:
        spark: Sessão ativa do Spark.
        dataframe: DataFrame que será alinhado.
        tabela_destino: Nome completo da tabela de destino.

    Returns:
        DataFrame com as mesmas colunas, ordem e tipos da tabela.
    """
    schema_destino = spark.table(tabela_destino).schema
    colunas_origem = set(dataframe.columns)

    expressoes = []

    for campo in schema_destino.fields:
        if campo.name in colunas_origem:
            expressoes.append(
                F.col(campo.name)
                .cast(campo.dataType)
                .alias(campo.name)
            )
        else:
            expressoes.append(
                F.lit(None)
                .cast(campo.dataType)
                .alias(campo.name)
            )

    return dataframe.select(*expressoes)

# COMMAND ----------


def gravar_tabela_sobrescrevendo(
    dataframe: DataFrame,
    tabela_destino: str,
) -> None:
    """
    Sobrescreve uma tabela Delta atualizando seu schema.

    Args:
        dataframe: DataFrame que será persistido.
        tabela_destino: Nome completo da tabela de destino.
    """
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_destino)
    )