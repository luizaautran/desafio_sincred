# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Camada Prata 
# MAGIC
# MAGIC Versão revisada para **Databricks Serverless**, com foco em estabilidade e entrega.
# MAGIC
# MAGIC
# MAGIC - Não utiliza RDD.
# MAGIC - Trata decimais no padrão brasileiro e internacional.
# MAGIC - Cria a tabela de métricas com schema explícito.
# MAGIC - Evita `DELTA_METADATA_MISMATCH` alinhando o schema antes de gravações.
# MAGIC - Mantém quarentena, deduplicação, SCD Tipo 2 e merges idempotentes.
# MAGIC - Permite executar uma fonte por vez.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from produto_transacional.qualidade.quarentena import (
    gravar_quarentena,
)

from produto_transacional.qualidade.validacoes import (
    adicionar_motivo_quarentena,
    separar_validos_invalidos,
)

from produto_transacional.prata.merge import (
    executar_merge,
)

from produto_transacional.prata.scd import (
    executar_scd_tipo_2,
)

from produto_transacional.utilitarios.dataframe import (
    converter_decimal_seguro,
    deduplicar,
)

from produto_transacional.utilitarios.tabelas import (
    alinhar_schema_com_tabela,
    tabela_existe,
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
        spark,
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

    executar_scd_tipo_2(
        spark=spark,
        dataframe=validos,
        tabela_destino=f"{CATALOGO}.{SCHEMA_PRATA}.clientes",
        chave_negocio="id_cliente",
        colunas_atributos=[
            "cpf",
            "nome",
            "cidade",
            "estado",
            "renda",
        ],
        coluna_data_evento="data_atualizacao",
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
            (F.col("tipo_conta").isNull(), "tipo_conta ausente"),
        ],
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
    
    executar_scd_tipo_2(
        spark=spark,
        dataframe=validos,
        tabela_destino=f"{CATALOGO}.{SCHEMA_PRATA}.contas",
        chave_negocio="id_conta",
        colunas_atributos=[
            "id_cliente",
            "tipo_conta",
            "status_conta",
        ],
        coluna_data_evento="data_atualizacao",
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

    executar_scd_tipo_2(
        spark=spark,
        dataframe=validos,
        tabela_destino=f"{CATALOGO}.{SCHEMA_PRATA}.cartoes",
        chave_negocio="id_cartao",
        colunas_atributos=[
            "id_conta",
            "tipo_cartao",
            "limite",
            "status_cartao",
        ],
        coluna_data_evento="data_atualizacao",
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

    executar_merge(
        spark=spark,
        dataframe=validos,
        tabela_destino=nome_tabela_destino,
        chave_negocio="id_estorno",
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

    if tabela_existe(spark, nome_completo):
        print(
            nome_completo,
            spark.table(nome_completo).count()
        )
    else:
        print(f"Não criada: {nome_completo}")