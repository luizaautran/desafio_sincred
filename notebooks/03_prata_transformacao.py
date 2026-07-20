# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Camada Prata Simplificada V2
# MAGIC
# MAGIC Versão revisada para **Databricks Serverless**, com foco em estabilidade e entrega.
# MAGIC
# MAGIC ## O que esta versão corrige
# MAGIC
# MAGIC - Não utiliza RDD.
# MAGIC - Trata decimais no padrão brasileiro e internacional.
# MAGIC - Cria a tabela de métricas com schema explícito.
# MAGIC - Evita `DELTA_METADATA_MISMATCH` alinhando o schema antes de gravações.
# MAGIC - Mantém quarentena, deduplicação, SCD Tipo 2 e merges idempotentes.
# MAGIC - Permite executar uma fonte por vez.
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)


# COMMAND ----------

CATALOGO = "workspace"

SCHEMA_BRONZE = "bronze"
SCHEMA_PRATA = "prata"
SCHEMA_QUARENTENA = "quarentena"
SCHEMA_OBSERVABILIDADE = "observabilidade"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_PRATA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_QUARENTENA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{SCHEMA_OBSERVABILIDADE}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções utilitárias

# COMMAND ----------

def tabela_existe(nome_tabela: str) -> bool:
    return spark.catalog.tableExists(nome_tabela)


def converter_decimal_seguro(
    nome_coluna: str,
    precisao: int = 18,
    escala: int = 2
):
    """
    Aceita:
    23221,69
    23.221,69
    23221.69
    Valores inválidos retornam NULL.
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


def coluna_timestamp_segura(nome_coluna: str):
    return F.coalesce(
        F.to_timestamp(F.col(nome_coluna)),
        F.current_timestamp()
    )


def adicionar_motivo_quarentena(
    dataframe: DataFrame,
    regras: list[tuple]
) -> DataFrame:
    motivos = [
        F.when(condicao, F.lit(mensagem))
        for condicao, mensagem in regras
    ]

    return dataframe.withColumn(
        "motivo_quarentena",
        F.concat_ws("; ", *motivos)
    )


def separar_validos_invalidos(
    dataframe: DataFrame
) -> tuple[DataFrame, DataFrame]:
    invalidos = dataframe.filter(
        F.length(F.trim(F.col("motivo_quarentena"))) > 0
    )

    validos = dataframe.filter(
        F.length(F.trim(F.col("motivo_quarentena"))) == 0
    ).drop("motivo_quarentena")

    return validos, invalidos




def deduplicar(
    dataframe: DataFrame,
    chaves: list[str],
    coluna_ordenacao: str
) -> DataFrame:
    from pyspark.sql.window import Window

    criterios = [
        F.col(coluna_ordenacao).desc_nulls_last()
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
            "numero_linha",
            F.row_number().over(janela)
        )
        .filter(F.col("numero_linha") == 1)
        .drop("numero_linha")
    )

def alinhar_schema_com_tabela(
    dataframe: DataFrame,
    tabela_destino: str
) -> DataFrame:
    """
    Seleciona e converte as colunas conforme o schema Delta existente.
    Isso reduz erros de metadata/schema em append.
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


def gravar_tabela_sobrescrevendo(
    dataframe: DataFrame,
    tabela_destino: str
) -> None:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabela_destino)
    )


def gravar_quarentena(
    dataframe: DataFrame,
    tabela_destino: str
) -> None:
    if dataframe.isEmpty():
        print(f"Sem registros inválidos: {tabela_destino}")
        return

    dados = dataframe.withColumn(
        "data_quarentena",
        F.current_timestamp()
    )

    if not tabela_existe(tabela_destino):
        gravar_tabela_sobrescrevendo(
            dados,
            tabela_destino
        )
    else:
        (
            dados.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(tabela_destino)
        )

    print(f"Quarentena atualizada: {tabela_destino}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Métricas com schema explícito

# COMMAND ----------

def garantir_tabela_metricas() -> str:
    tabela_metricas = (
        f"{CATALOGO}.{SCHEMA_OBSERVABILIDADE}."
        "metricas_qualidade_prata"
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tabela_metricas} (
            fonte STRING,
            quantidade_entrada BIGINT,
            quantidade_validos BIGINT,
            quantidade_invalidos BIGINT,
            data_processamento TIMESTAMP
        )
        USING DELTA
    """)

    return tabela_metricas


def registrar_metrica(
    fonte: str,
    quantidade_entrada: int,
    quantidade_validos: int,
    quantidade_invalidos: int
) -> None:
    tabela_metricas = garantir_tabela_metricas()

    schema_metricas = StructType([
        StructField("fonte", StringType(), False),
        StructField("quantidade_entrada", LongType(), False),
        StructField("quantidade_validos", LongType(), False),
        StructField("quantidade_invalidos", LongType(), False),
    ])

    dados = [(
        str(fonte),
        int(quantidade_entrada),
        int(quantidade_validos),
        int(quantidade_invalidos)
    )]

    dataframe_metricas = (
        spark.createDataFrame(
            dados,
            schema=schema_metricas
        )
        .withColumn(
            "data_processamento",
            F.current_timestamp().cast(TimestampType())
        )
        .select(
            "fonte",
            "quantidade_entrada",
            "quantidade_validos",
            "quantidade_invalidos",
            "data_processamento"
        )
    )

    dataframe_metricas = alinhar_schema_com_tabela(
        dataframe_metricas,
        tabela_metricas
    )

    (
        dataframe_metricas.write
        .format("delta")
        .mode("append")
        .saveAsTable(tabela_metricas)
    )

    print(f"Métrica registrada: {fonte}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Tipo 2

# COMMAND ----------

def aplicar_scd_tipo_2(
    dataframe: DataFrame,
    tabela_destino: str,
    chave_negocio: str,
    colunas_atributos: list[str],
    coluna_data_evento: str
) -> None:
    colunas_hash = [
        F.coalesce(
            F.col(coluna).cast("string"),
            F.lit("∅")
        )
        for coluna in sorted(colunas_atributos)
    ]

    origem = (
        dataframe
        .withColumn(
            "hash_atributos",
            F.sha2(
                F.concat_ws("||", *colunas_hash),
                256
            )
        )
        .withColumn(
            "data_inicio_vigencia",
            F.coalesce(
                F.col(coluna_data_evento).cast("timestamp"),
                F.current_timestamp()
            )
        )
        .withColumn(
            "data_fim_vigencia",
            F.lit(None).cast("timestamp")
        )
        .withColumn(
            "registro_atual",
            F.lit(True).cast("boolean")
        )
    )

    colunas_obrigatorias = {
        chave_negocio,
        "hash_atributos",
        "data_inicio_vigencia",
        "data_fim_vigencia",
        "registro_atual"
    }

    if not tabela_existe(tabela_destino):
        gravar_tabela_sobrescrevendo(
            origem,
            tabela_destino
        )
        print(f"Tabela SCD criada: {tabela_destino}")
        return

    colunas_existentes = set(
        spark.table(tabela_destino).columns
    )

    if not colunas_obrigatorias.issubset(colunas_existentes):
        gravar_tabela_sobrescrevendo(
            origem,
            tabela_destino
        )
        print(f"Tabela SCD recriada: {tabela_destino}")
        return

    registros_atuais = (
        spark.table(tabela_destino)
        .filter(F.col("registro_atual") == True)
        .select(
            F.col(chave_negocio).alias("chave_destino"),
            F.col("hash_atributos").alias("hash_destino")
        )
    )

    alteracoes = (
        origem.alias("origem")
        .join(
            registros_atuais.alias("destino"),
            F.col(f"origem.{chave_negocio}")
            == F.col("destino.chave_destino"),
            "left"
        )
        .filter(
            F.col("hash_destino").isNull()
            | (
                F.col("origem.hash_atributos")
                != F.col("hash_destino")
            )
        )
        .select("origem.*")
    )

    if alteracoes.isEmpty():
        print(f"Sem alterações em: {tabela_destino}")
        return

    tabela_delta = DeltaTable.forName(
        spark,
        tabela_destino
    )

    (
        tabela_delta.alias("destino")
        .merge(
            alteracoes.select(
                chave_negocio,
                "data_inicio_vigencia"
            ).distinct().alias("origem"),
            f"""
            destino.{chave_negocio} = origem.{chave_negocio}
            AND destino.registro_atual = true
            """
        )
        .whenMatchedUpdate(
            set={
                "registro_atual": "false",
                "data_fim_vigencia":
                    "origem.data_inicio_vigencia - INTERVAL 1 MICROSECOND"
            }
        )
        .execute()
    )

    alteracoes_alinhadas = alinhar_schema_com_tabela(
        alteracoes,
        tabela_destino
    )

    (
        alteracoes_alinhadas.write
        .format("delta")
        .mode("append")
        .saveAsTable(tabela_destino)
    )

    print(f"SCD atualizado: {tabela_destino}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge idempotente para fatos

# COMMAND ----------

def aplicar_merge(
    dataframe: DataFrame,
    tabela_destino: str,
    chave_negocio: str
) -> None:
    if not tabela_existe(tabela_destino):
        gravar_tabela_sobrescrevendo(
            dataframe,
            tabela_destino
        )
        print(f"Tabela criada: {tabela_destino}")
        return

    colunas_destino = spark.table(
        tabela_destino
    ).columns

    dataframe_alinhado = alinhar_schema_com_tabela(
        dataframe,
        tabela_destino
    )

    tabela_delta = DeltaTable.forName(
        spark,
        tabela_destino
    )

    (
        tabela_delta.alias("destino")
        .merge(
            dataframe_alinhado.alias("origem"),
            f"destino.{chave_negocio} = origem.{chave_negocio}"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"Merge concluído: {tabela_destino}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento de clientes

# COMMAND ----------

def processar_clientes() -> None:
    origem = spark.table(
        f"{CATALOGO}.{SCHEMA_BRONZE}.clientes"
    )

    dados = (
        origem
        .withColumn("id_cliente", F.trim(F.col("id_cliente")))
        .withColumn("cpf", F.regexp_replace(F.col("cpf"), r"\D", ""))
        .withColumn("nome", F.initcap(F.trim(F.col("nome"))))
        .withColumn("cidade", F.initcap(F.trim(F.col("cidade"))))
        .withColumn("estado", F.upper(F.trim(F.col("estado"))))
        .withColumn("renda", converter_decimal_seguro("renda"))
        .withColumn(
            "data_atualizacao",
            F.coalesce(
                F.to_timestamp("data_atualizacao"),
                F.to_timestamp("data_ingestao"),
                F.current_timestamp()
            )
        )
    )

    dados = adicionar_motivo_quarentena(
        dados,
        [
            (F.col("id_cliente").isNull(), "id_cliente ausente"),
            (F.length(F.col("cpf")) != 11, "cpf inválido"),
            (F.col("nome").isNull(), "nome ausente"),
            (F.col("renda").isNull(), "renda inválida")
        ]
    )

    validos, invalidos = separar_validos_invalidos(dados)

    validos = deduplicar(
        validos,
        ["id_cliente"],
        "data_atualizacao"
    )

    gravar_quarentena(
        invalidos,
        f"{CATALOGO}.{SCHEMA_QUARENTENA}.clientes"
    )

    aplicar_scd_tipo_2(
        validos,
        f"{CATALOGO}.{SCHEMA_PRATA}.clientes",
        "id_cliente",
        ["cpf", "nome", "cidade", "estado", "renda"],
        "data_atualizacao"
    )

    registrar_metrica(
        "clientes",
        origem.count(),
        validos.count(),
        invalidos.count()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento de contas

# COMMAND ----------

def processar_contas() -> None:
    origem = spark.table(
        f"{CATALOGO}.{SCHEMA_BRONZE}.contas"
    )

    dados = (
        origem
        .withColumn("id_conta", F.trim(F.col("id_conta")))
        .withColumn("id_cliente", F.trim(F.col("id_cliente")))
        .withColumn("tipo_conta", F.upper(F.trim(F.col("tipo_conta"))))
        .withColumn("status_conta", F.upper(F.trim(F.col("status_conta"))))
        .withColumn(
            "data_atualizacao",
            F.coalesce(
                F.to_timestamp("data_atualizacao"),
                F.to_timestamp("data_ingestao"),
                F.current_timestamp()
            )
        )
    )

    dados = adicionar_motivo_quarentena(
        dados,
        [
            (F.col("id_conta").isNull(), "id_conta ausente"),
            (F.col("id_cliente").isNull(), "id_cliente ausente"),
            (F.col("tipo_conta").isNull(), "tipo_conta ausente")        ]
    )

    validos, invalidos = separar_validos_invalidos(dados)

    validos = deduplicar(
        validos,
        ["id_conta"],
        "data_atualizacao"
    )

    gravar_quarentena(
        invalidos,
        f"{CATALOGO}.{SCHEMA_QUARENTENA}.contas"
    )

    aplicar_scd_tipo_2(
        validos,
        f"{CATALOGO}.{SCHEMA_PRATA}.contas",
        "id_conta",
        ["id_cliente", "tipo_conta",  "status_conta"],
        "data_atualizacao"
    )

    registrar_metrica(
        "contas",
        origem.count(),
        validos.count(),
        invalidos.count()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento de cartões

# COMMAND ----------

def processar_cartoes() -> None:
    origem = spark.table(
        f"{CATALOGO}.{SCHEMA_BRONZE}.cartoes"
    )

    dados = (
        origem
        .withColumn("id_cartao", F.trim(F.col("id_cartao")))
        .withColumn("id_conta", F.trim(F.col("id_conta")))
        .withColumn("tipo_cartao", F.upper(F.trim(F.col("tipo_cartao"))))
        .withColumn("status_cartao", F.upper(F.trim(F.col("status_cartao"))))
        .withColumn("limite", converter_decimal_seguro("limite"))
        .withColumn(
            "data_atualizacao",
            F.coalesce(
                F.to_timestamp("data_atualizacao"),
                F.to_timestamp("data_ingestao"),
                F.current_timestamp()
            )
        )
    )

    dados = adicionar_motivo_quarentena(
        dados,
        [
            (F.col("id_cartao").isNull(), "id_cartao ausente"),
            (F.col("id_conta").isNull(), "id_conta ausente"),
            (F.col("limite").isNull(), "limite inválido")
        ]
    )

    validos, invalidos = separar_validos_invalidos(dados)

    validos = deduplicar(
        validos,
        ["id_cartao"],
        "data_atualizacao"
    )

    gravar_quarentena(
        invalidos,
        f"{CATALOGO}.{SCHEMA_QUARENTENA}.cartoes"
    )

    aplicar_scd_tipo_2(
        validos,
        f"{CATALOGO}.{SCHEMA_PRATA}.cartoes",
        "id_cartao",
        ["id_conta", "tipo_cartao", "limite", "status_cartao"],
        "data_atualizacao"
    )

    registrar_metrica(
        "cartoes",
        origem.count(),
        validos.count(),
        invalidos.count()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento de transações

# COMMAND ----------

def processar_transacoes():

    nome_tabela_origem = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.transacoes"
    )

    nome_tabela_cartoes = (
        f"{CATALOGO}.{SCHEMA_PRATA}.cartoes"
    )

    nome_tabela_destino = (
        f"{CATALOGO}.{SCHEMA_PRATA}.transacoes"
    )

    nome_tabela_quarentena = (
        f"{CATALOGO}.{SCHEMA_QUARENTENA}.transacoes"
    )

    # ============================================================
    # 1. Função auxiliar para colunas opcionais
    # ============================================================

    def coluna_opcional(
        dataframe,
        nome_coluna,
        tipo="string"
    ):
        if nome_coluna in dataframe.columns:
            return F.col(nome_coluna).cast(tipo)

        return F.lit(None).cast(tipo)

    # ============================================================
    # 2. Leitura da origem
    # ============================================================

    transacoes_origem = spark.table(
        nome_tabela_origem
    )

    cartoes_prata = (
        spark.table(nome_tabela_cartoes)
        .select(
            F.col("id_cartao")
            .cast("string")
            .alias("id_cartao"),

            F.col("id_conta")
            .cast("string")
            .alias("id_conta")
        )
        .dropDuplicates(["id_cartao"])
    )

    # ============================================================
    # 3. Padronização das transações
    # ============================================================

    transacoes_padronizadas = (
        transacoes_origem
        .select(
            F.col("id_transacao")
            .cast("string")
            .alias("id_transacao"),

            F.col("id_cartao")
            .cast("string")
            .alias("id_cartao"),

            F.to_timestamp(
                F.col("data_transacao")
            ).alias("data_transacao"),

            F.expr(
                """
                try_cast(
                    replace(
                        cast(valor as string),
                        ',',
                        '.'
                    )
                    as decimal(18,2)
                )
                """
            ).alias("valor"),

            F.trim(
                F.col("estabelecimento")
            ).alias("estabelecimento"),

            F.upper(
                F.trim(F.col("canal"))
            ).alias("canal"),

            F.upper(
                F.trim(F.col("pais"))
            ).alias("pais"),

            F.upper(
                F.trim(F.col("moeda"))
            ).alias("moeda"),

            F.col("mcc")
            .cast("string")
            .alias("mcc"),

            coluna_opcional(
                transacoes_origem,
                "data_hora_ingestao",
                "timestamp"
            ).alias("data_hora_ingestao"),

            coluna_opcional(
                transacoes_origem,
                "arquivo_origem",
                "string"
            ).alias("arquivo_origem"),

            coluna_opcional(
                transacoes_origem,
                "id_lote",
                "string"
            ).alias("id_lote"),

            coluna_opcional(
                transacoes_origem,
                "nome_arquivo_origem",
                "string"
            ).alias("nome_arquivo_origem"),

            coluna_opcional(
                transacoes_origem,
                "tamanho_arquivo_origem",
                "long"
            ).alias("tamanho_arquivo_origem"),

            coluna_opcional(
                transacoes_origem,
                "data_modificacao_arquivo",
                "timestamp"
            ).alias("data_modificacao_arquivo"),

            coluna_opcional(
                transacoes_origem,
                "data_ingestao",
                "date"
            ).alias("data_ingestao"),

            coluna_opcional(
                transacoes_origem,
                "timestamp_ingestao",
                "timestamp"
            ).alias("timestamp_ingestao"),

            coluna_opcional(
                transacoes_origem,
                "batch_id",
                "string"
            ).alias("batch_id"),

            coluna_opcional(
                transacoes_origem,
                "hash_linha",
                "string"
            ).alias("hash_linha"),

            coluna_opcional(
                transacoes_origem,
                "schema_version",
                "decimal(10,1)"
            ).alias("schema_version")
        )
    )

    # ============================================================
    # 4. Enriquecimento com cartão e conta
    # ============================================================

    dados_enriquecidos = (
        transacoes_padronizadas.alias("t")
        .join(
            cartoes_prata.alias("c"),
            F.col("t.id_cartao")
            == F.col("c.id_cartao"),
            "left"
        )
        .select(
            F.col("t.id_transacao"),
            F.col("t.id_cartao"),
            F.col("c.id_conta"),
            F.col("t.data_transacao"),
            F.col("t.valor"),
            F.col("t.estabelecimento"),
            F.col("t.canal"),
            F.col("t.pais"),
            F.col("t.moeda"),
            F.col("t.mcc"),
            F.col("t.data_hora_ingestao"),
            F.col("t.arquivo_origem"),
            F.col("t.id_lote"),
            F.col("t.nome_arquivo_origem"),
            F.col("t.tamanho_arquivo_origem"),
            F.col("t.data_modificacao_arquivo"),
            F.col("t.data_ingestao"),
            F.col("t.timestamp_ingestao"),
            F.col("t.batch_id"),
            F.col("t.hash_linha"),
            F.col("t.schema_version")
        )
    )

    # ============================================================
    # 5. Regras de qualidade
    # ============================================================

    dados_classificados = (
        dados_enriquecidos
        .withColumn(
            "motivo_quarentena",
            F.when(
                F.col("id_transacao").isNull()
                | (F.trim(F.col("id_transacao")) == ""),
                F.lit("ID_TRANSACAO_NULO")
            )
            .when(
                F.col("id_cartao").isNull()
                | (F.trim(F.col("id_cartao")) == ""),
                F.lit("ID_CARTAO_NULO")
            )
            .when(
                F.col("id_conta").isNull(),
                F.lit("CARTAO_NAO_ENCONTRADO")
            )
            .when(
                F.col("data_transacao").isNull(),
                F.lit("DATA_TRANSACAO_INVALIDA")
            )
            .when(
                F.col("valor").isNull(),
                F.lit("VALOR_INVALIDO")
            )
            .when(
                F.col("valor") <= 0,
                F.lit("VALOR_NAO_POSITIVO")
            )
            .when(
                F.col("estabelecimento").isNull()
                | (
                    F.trim(
                        F.col("estabelecimento")
                    ) == ""
                ),
                F.lit("ESTABELECIMENTO_NULO")
            )
            .when(
                F.col("canal").isNull()
                | (F.trim(F.col("canal")) == ""),
                F.lit("CANAL_NULO")
            )
        )
    )

    # ============================================================
    # 6. Válidos e quarentena
    # ============================================================

    dados_validos = (
        dados_classificados
        .filter(
            F.col("motivo_quarentena").isNull()
        )
        .drop("motivo_quarentena")
        .dropDuplicates(["id_transacao"])
    )

    dados_quarentena = (
        dados_classificados
        .filter(
            F.col("motivo_quarentena").isNotNull()
        )
        .dropDuplicates(["id_transacao"])
    )

    # ============================================================
    # 7. Escrita
    # ============================================================

    (
        dados_validos.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela_destino)
    )

    (
        dados_quarentena.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela_quarentena)
    )

    # ============================================================
    # 8. Métricas
    # ============================================================

    quantidade_origem = transacoes_origem.count()
    quantidade_validos = dados_validos.count()
    quantidade_quarentena = dados_quarentena.count()

    print("=" * 60)
    print("PROCESSAMENTO DE TRANSAÇÕES CONCLUÍDO")
    print("=" * 60)
    print(f"Origem: {nome_tabela_origem}")
    print(f"Destino: {nome_tabela_destino}")
    print(f"Quarentena: {nome_tabela_quarentena}")
    print(f"Registros na origem: {quantidade_origem}")
    print(f"Registros válidos: {quantidade_validos}")
    print(
        f"Registros em quarentena: "
        f"{quantidade_quarentena}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento de eventos de risco

# COMMAND ----------

from pyspark.sql import functions as F


def processar_eventos_risco():

    nome_tabela_origem = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.eventos_risco"
    )

    nome_tabela_transacoes = (
        f"{CATALOGO}.{SCHEMA_PRATA}.transacoes"
    )

    nome_tabela_destino = (
        f"{CATALOGO}.{SCHEMA_PRATA}.eventos_risco"
    )

    nome_tabela_quarentena = (
        f"{CATALOGO}.{SCHEMA_QUARENTENA}.eventos_risco"
    )

    # ============================================================
    # 1. Função auxiliar para colunas opcionais
    # ============================================================

    def coluna_opcional(
        dataframe,
        nome_coluna,
        tipo="string"
    ):
        if nome_coluna in dataframe.columns:
            return F.expr(
                f"try_cast(`{nome_coluna}` AS {tipo})"
            )

        return F.lit(None).cast(tipo)

    # ============================================================
    # 2. Leitura das tabelas
    # ============================================================

    eventos_origem = spark.table(
        nome_tabela_origem
    )

    transacoes_prata = (
        spark.table(nome_tabela_transacoes)
        .select(
            F.col("id_transacao")
            .cast("string")
            .alias("id_transacao"),

            F.col("id_cartao")
            .cast("string")
            .alias("id_cartao"),

            F.col("id_conta")
            .cast("string")
            .alias("id_conta")
        )
        .dropDuplicates(["id_transacao"])
    )

    # ============================================================
    # 3. Padronização dos eventos de risco
    # ============================================================

    eventos_padronizados = (
        eventos_origem
        .select(
            F.col("id_evento")
            .cast("string")
            .alias("id_evento"),

            F.col("id_transacao")
            .cast("string")
            .alias("id_transacao"),

            F.upper(
                F.trim(F.col("tipo_evento"))
            ).alias("tipo_evento"),

            F.upper(
                F.trim(F.col("severidade"))
            ).alias("severidade"),

            # Conversão segura: valores inválidos viram NULL
            F.expr(
                "try_cast(`data_evento` AS timestamp)"
            ).alias("data_evento"),

            coluna_opcional(
                eventos_origem,
                "data_hora_ingestao",
                "timestamp"
            ).alias("data_hora_ingestao"),

            coluna_opcional(
                eventos_origem,
                "arquivo_origem",
                "string"
            ).alias("arquivo_origem"),

            coluna_opcional(
                eventos_origem,
                "id_lote",
                "string"
            ).alias("id_lote"),

            coluna_opcional(
                eventos_origem,
                "nome_arquivo_origem",
                "string"
            ).alias("nome_arquivo_origem"),

            coluna_opcional(
                eventos_origem,
                "tamanho_arquivo_origem",
                "bigint"
            ).alias("tamanho_arquivo_origem"),

            coluna_opcional(
                eventos_origem,
                "data_modificacao_arquivo",
                "timestamp"
            ).alias("data_modificacao_arquivo"),

            coluna_opcional(
                eventos_origem,
                "data_ingestao",
                "date"
            ).alias("data_ingestao"),

            coluna_opcional(
                eventos_origem,
                "timestamp_ingestao",
                "timestamp"
            ).alias("timestamp_ingestao"),

            coluna_opcional(
                eventos_origem,
                "batch_id",
                "string"
            ).alias("batch_id"),

            coluna_opcional(
                eventos_origem,
                "hash_linha",
                "string"
            ).alias("hash_linha"),

            coluna_opcional(
                eventos_origem,
                "schema_version",
                "decimal(10,1)"
            ).alias("schema_version")
        )
    )

    # ============================================================
    # 4. Enriquecimento com transação, cartão e conta
    # ============================================================

    dados_enriquecidos = (
        eventos_padronizados.alias("e")
        .join(
            transacoes_prata.alias("t"),
            F.col("e.id_transacao")
            == F.col("t.id_transacao"),
            "left"
        )
        .select(
            F.col("e.id_evento"),
            F.col("e.id_transacao"),
            F.col("t.id_cartao"),
            F.col("t.id_conta"),
            F.col("e.tipo_evento"),
            F.col("e.severidade"),
            F.col("e.data_evento"),
            F.col("e.data_hora_ingestao"),
            F.col("e.arquivo_origem"),
            F.col("e.id_lote"),
            F.col("e.nome_arquivo_origem"),
            F.col("e.tamanho_arquivo_origem"),
            F.col("e.data_modificacao_arquivo"),
            F.col("e.data_ingestao"),
            F.col("e.timestamp_ingestao"),
            F.col("e.batch_id"),
            F.col("e.hash_linha"),
            F.col("e.schema_version")
        )
    )

    # ============================================================
    # 5. Regras de qualidade
    # ============================================================

    severidades_validas = [
        "BAIXA",
        "MEDIA",
        "ALTA",
        "CRITICA"
    ]

    dados_classificados = (
        dados_enriquecidos
        .withColumn(
            "motivo_quarentena",
            F.when(
                F.col("id_evento").isNull()
                | (F.trim(F.col("id_evento")) == ""),
                F.lit("ID_EVENTO_NULO")
            )
            .when(
                F.col("id_transacao").isNull()
                | (F.trim(F.col("id_transacao")) == ""),
                F.lit("ID_TRANSACAO_NULO")
            )
            .when(
                F.col("id_cartao").isNull(),
                F.lit("TRANSACAO_NAO_ENCONTRADA")
            )
            .when(
                F.col("tipo_evento").isNull()
                | (F.trim(F.col("tipo_evento")) == ""),
                F.lit("TIPO_EVENTO_NULO")
            )
            .when(
                F.col("severidade").isNull()
                | ~F.col("severidade").isin(
                    severidades_validas
                ),
                F.lit("SEVERIDADE_INVALIDA")
            )
            .when(
                F.col("data_evento").isNull(),
                F.lit("DATA_EVENTO_INVALIDA")
            )
        )
    )

    # ============================================================
    # 6. Separação entre válidos e quarentena
    # ============================================================

    dados_validos = (
        dados_classificados
        .filter(
            F.col("motivo_quarentena").isNull()
        )
        .drop("motivo_quarentena")
        .dropDuplicates(["id_evento"])
    )

    dados_quarentena = (
        dados_classificados
        .filter(
            F.col("motivo_quarentena").isNotNull()
        )
        .dropDuplicates(["id_evento"])
    )

    # ============================================================
    # 7. Escrita das tabelas
    # ============================================================

    (
        dados_validos.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela_destino)
    )

    (
        dados_quarentena.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(nome_tabela_quarentena)
    )

    # ============================================================
    # 8. Métricas
    # ============================================================

    quantidade_origem = eventos_origem.count()
    quantidade_validos = dados_validos.count()
    quantidade_quarentena = dados_quarentena.count()

    print("=" * 60)
    print("PROCESSAMENTO DE EVENTOS DE RISCO CONCLUÍDO")
    print("=" * 60)
    print(f"Origem: {nome_tabela_origem}")
    print(f"Destino: {nome_tabela_destino}")
    print(f"Quarentena: {nome_tabela_quarentena}")
    print(f"Registros na origem: {quantidade_origem}")
    print(f"Registros válidos: {quantidade_validos}")
    print(
        f"Registros em quarentena: "
        f"{quantidade_quarentena}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processamento de estornos

# COMMAND ----------

def processar_estornos() -> None:
    nome_tabela_origem = (
        f"{CATALOGO}.{SCHEMA_BRONZE}.estornos"
    )

    nome_tabela_destino = (
        f"{CATALOGO}.{SCHEMA_PRATA}.estornos"
    )

    nome_tabela_quarentena = (
        f"{CATALOGO}.{SCHEMA_QUARENTENA}.estornos"
    )

    origem = spark.table(nome_tabela_origem)

    dados = (
        origem
        .withColumn(
            "id_estorno",
            F.trim(
                F.col("id_estorno").cast("string")
            )
        )
        .withColumn(
            "id_transacao",
            F.trim(
                F.col("id_transacao").cast("string")
            )
        )
        .withColumn(
            "valor_estorno",
            converter_decimal_seguro("valor_estorno")
        )
        .withColumn(
            "data_estorno",
            F.expr(
                "try_cast(`data_estorno` AS timestamp)"
            )
        )
    )

    dados = adicionar_motivo_quarentena(
        dados,
        [
            (
                F.col("id_estorno").isNull()
                | (F.col("id_estorno") == ""),
                "id_estorno ausente"
            ),
            (
                F.col("id_transacao").isNull()
                | (F.col("id_transacao") == ""),
                "id_transacao ausente"
            ),
            (
                F.col("valor_estorno").isNull(),
                "valor_estorno inválido"
            ),
            (
                F.col("valor_estorno") <= 0,
                "valor_estorno deve ser maior que zero"
            ),
            (
                F.col("data_estorno").isNull(),
                "data_estorno inválida"
            )
        ]
    )

    validos, invalidos = separar_validos_invalidos(dados)

    validos = deduplicar(
        validos,
        ["id_estorno"],
        "data_estorno"
    )

    gravar_quarentena(
        invalidos,
        nome_tabela_quarentena
    )

    aplicar_merge(
        validos,
        nome_tabela_destino,
        "id_estorno"
    )

    quantidade_origem = origem.count()
    quantidade_validos = validos.count()
    quantidade_invalidos = invalidos.count()

    registrar_metrica(
        "estornos",
        quantidade_origem,
        quantidade_validos,
        quantidade_invalidos
    )

    print("=" * 60)
    print("PROCESSAMENTO DE ESTORNOS CONCLUÍDO")
    print("=" * 60)
    print(f"Origem: {nome_tabela_origem}")
    print(f"Destino: {nome_tabela_destino}")
    print(f"Quarentena: {nome_tabela_quarentena}")
    print(f"Registros na origem: {quantidade_origem}")
    print(f"Registros válidos: {quantidade_validos}")
    print(f"Registros inválidos: {quantidade_invalidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Execução controlada
# MAGIC
# MAGIC Execute primeiro somente clientes. Depois, avance uma fonte por vez.
# MAGIC

# COMMAND ----------

processar_clientes()


# COMMAND ----------

# Execute após clientes concluir:
processar_contas()


# COMMAND ----------

# Execute após contas concluir:
processar_cartoes()


# COMMAND ----------

# Execute após cartões concluir:
processar_transacoes()


# COMMAND ----------

# Execute após transações concluir:
processar_eventos_risco()


# COMMAND ----------

# Execute após eventos concluir:
processar_estornos()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação final

# COMMAND ----------

tabelas_prata = [
    "clientes",
    "contas",
    "cartoes",
    "transacoes",
    "eventos_risco",
    "estornos"
]

for tabela in tabelas_prata:
    nome_completo = f"{CATALOGO}.{SCHEMA_PRATA}.{tabela}"

    if tabela_existe(nome_completo):
        print(
            nome_completo,
            spark.table(nome_completo).count()
        )
    else:
        print(f"Não criada: {nome_completo}")
