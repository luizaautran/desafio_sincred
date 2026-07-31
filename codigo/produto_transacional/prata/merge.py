
# MAGIC %md
# MAGIC # Produto Transacional — Merge Delta
# MAGIC
# MAGIC Módulo responsável pelos merges idempotentes da camada Prata.
# MAGIC
# MAGIC ## Responsabilidades
# MAGIC
# MAGIC - Executar merges em tabelas Delta.
# MAGIC - Atualizar registros existentes.
# MAGIC - Inserir novos registros.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

# COMMAND ----------


def executar_merge(
    dataframe: DataFrame,
    tabela_destino: str,
    condicao_merge: str,
    colunas_atualizacao: dict[str, str],
) -> None:
    """
    Executa um merge entre um DataFrame e uma tabela Delta.
    """

    delta_table = DeltaTable.forName(
        dataframe.sparkSession,
        tabela_destino,
    )

    (
        delta_table.alias("destino")
        .merge(
            dataframe.alias("origem"),
            condicao_merge,
        )
        .whenMatchedUpdate(
            set=colunas_atualizacao,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )